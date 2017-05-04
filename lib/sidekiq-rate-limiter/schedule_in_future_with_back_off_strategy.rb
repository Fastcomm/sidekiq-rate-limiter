module Sidekiq::RateLimiter
  class ScheduleInFutureWithBackOffStrategy
    def call(work, klass, args, rate_limited_count, limit_options)
      Sidekiq.redis do |conn|
        lim = Limit.new(conn, limit_options)
        if lim.exceeded?(klass)
          rate_limited_count += 1 if rate_limited_count < 10

          limit_expires_in = lim.retry_in?(klass)

          # schedule retry for a multiple of the number of times the job has been attempted.
          # this is a basic back-off function which tries to schedule the job at a future time when
          # the system is less busy.
          retry_in = (limit_expires_in + rand(limit_expires_in)) * rate_limited_count

          # Schedule the job to be executed in the future, when we think the rate limit might be back to normal.
          enqueue_to_in(work.queue_name, retry_in, Object.const_get(klass), rate_limited_count, *args)
          nil
        else
          lim.add(klass)
          work
        end
      end

    end

    def enqueue_to_in(queue, interval, klass, rate_limited_count, *args)
      int = interval.to_f
      now = Time.now.to_f
      ts = (int < 1_000_000_000 ? now + int : int)

      item = { 'class'.freeze => klass, 'args'.freeze => args, 'at'.freeze => ts, 'queue'.freeze => queue, 'rate_limited_count'.freeze => rate_limited_count }
      item.delete('at'.freeze) if ts <= now

      klass.client_push(item)
    end
  end
end

