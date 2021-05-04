require "weak_ref"

# This code is from "crystal/db".

# :nodoc:
class Mongo::Connection::Pool(T)
  # Pool configuration

  # initial number of connections in the pool
  @initial_pool_size : Int32
  # maximum amount of connections in the pool (Idle + InUse)
  @max_pool_size : Int32
  # maximum amount of idle connections in the pool
  @max_idle_pool_size : Int32
  # seconds to wait before timeout while doing a checkout
  @checkout_timeout : Float64

  # Pool state

  # total of open connections managed by this pool
  @total = [] of T
  # connections available for checkout
  @idle = Set(T).new
  # connections waiting to be stablished (they are not in *@idle* nor in *@total*)
  @inflight : Int32

  # Sync state

  # communicate that a connection is available for checkout
  @availability_channel : Channel(Nil)
  # global pool mutex
  @mutex : Mutex

  def initialize(@initial_pool_size = 1, @max_pool_size = 0, @max_idle_pool_size = 1, @checkout_timeout = 5.0,
                 &@factory : -> T)
    @availability_channel = Channel(Nil).new
    @inflight = 0
    @mutex = Mutex.new

    @initial_pool_size.times { build_resource }
  end

  # close all resources in the pool
  def close : Nil
    @total.each &.close
    @total.clear
    @idle.clear
  end

  record Stats,
    open_connections : Int32,
    idle_connections : Int32,
    in_flight_connections : Int32,
    max_connections : Int32

  # Returns stats of the pool
  def stats
    Stats.new(
      open_connections: @total.size,
      idle_connections: @idle.size,
      in_flight_connections: @inflight,
      max_connections: @max_pool_size,
    )
  end

  def checkout : T
    res = sync do
      resource = nil

      until resource
        resource = if @idle.empty?
                     if can_increase_pool?
                       @inflight += 1
                       r = unsync { build_resource }
                       @inflight -= 1
                       r
                     else
                       unsync { wait_for_available }
                       # The wait for available can unlock
                       # multiple fibers waiting for a resource.
                       # Although only one will pick it due to the lock
                       # in the end of the unsync, the pick_available
                       # will return nil
                       pick_available
                     end
                   else
                     pick_available
                   end
      end

      @idle.delete resource

      resource
    end

    if res.responds_to?(:before_checkout)
      res.before_checkout
    end
    res
  end

  def checkout(&block : T ->)
    connection = checkout

    begin
      yield connection
    ensure
      release connection
    end
  end

  # ```
  # selected, is_candidate = pool.checkout_some(candidates)
  # ```
  # `selected` be a resource from the `candidates` list and `is_candidate` == `true`
  # or `selected` will be a new resource and `is_candidate` == `false`
  def checkout_some(candidates : Enumerable(WeakRef(T))) : {T, Bool}
    sync do
      candidates.each do |ref|
        resource = ref.value
        if resource && is_available?(resource)
          @idle.delete resource
          resource.before_checkout
          return {resource, true}
        end
      end
    end

    resource = checkout
    {resource, candidates.any? { |ref| ref.value == resource }}
  end

  def release(resource : T) : Nil
    sync do
      if can_increase_idle_pool
        @idle << resource
        if resource.responds_to?(:after_release)
          resource.after_release
        end
        select
        when @availability_channel.send nil
          # send if someone is waiting…
        else
          # …but do not block.
        end
      else
        resource.close
        @total.delete(resource)
      end
    end
  end

  # :nodoc:
  def each_resource
    sync do
      @idle.each do |resource|
        yield resource
      end
    end
  end

  # :nodoc:
  def is_available?(resource : T)
    @idle.includes?(resource)
  end

  # :nodoc:
  def delete(resource : T)
    @total.delete(resource)
    @idle.delete(resource)
  end

  private def build_resource : T
    resource = @factory.call
    @total << resource
    @idle << resource
    resource
  end

  private def can_increase_pool?
    @max_pool_size == 0 || @total.size + @inflight < @max_pool_size
  end

  private def can_increase_idle_pool
    @idle.size < @max_idle_pool_size
  end

  private def pick_available
    @idle.first?
  end

  private def wait_for_available
    select
    when @availability_channel.receive
    when timeout(@checkout_timeout.seconds)
      raise Mongo::Error::Connection.new("Too many open connections, could not check out a connection in #{@checkout_timeout} seconds.")
    end
  end

  private def sync
    @mutex.lock
    begin
      yield
    ensure
      @mutex.unlock
    end
  end

  private def unsync
    @mutex.unlock
    begin
      yield
    ensure
      @mutex.lock
    end
  end
end
