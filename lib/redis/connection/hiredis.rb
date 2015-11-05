require "redis/connection/registry"
require "redis/errors"
require "hiredis/connection"
require "timeout"

class Redis
  module Connection
    class Hiredis
      
      RDS_READONLY_ERROR = "READONLY You can't write against a read only slave.".freeze
      RDS_READONLY_MESSAGE = "A write operation was issued to an RDS slave node.".freeze

      def self.connect(config)
        connection = ::Hiredis::Connection.new

        if config[:scheme] == "unix"
          connection.connect_unix(config[:path], Integer(config[:timeout] * 1_000_000))
        else
          connection.connect(config[:host], config[:port], Integer(config[:timeout] * 1_000_000))
        end

        instance = new(connection)
        instance.timeout = config[:timeout]
        instance
      rescue Errno::ETIMEDOUT
        raise TimeoutError
      end

      def initialize(connection)
        @connection = connection
      end

      def connected?
        @connection && @connection.connected?
      end

      def timeout=(timeout)
        # Hiredis works with microsecond timeouts
        @connection.timeout = Integer(timeout * 1_000_000)
      end

      def disconnect
        @connection.disconnect
        @connection = nil
      end

      def write(command)
        @connection.write(command.flatten(1))
      rescue Errno::EAGAIN
        raise TimeoutError
      end

      def read
        reply = @connection.read
        if reply.is_a?(RuntimeError)
          if reply.message.strip == RDS_READONLY_ERROR
            raise BaseConnectionError
          else
            reply = CommandError.new(reply.message) 
          end
        end
        reply
      rescue BaseConnectionError
        raise ConnectionError, RDS_READONLY_MESSAGE
      rescue Errno::EAGAIN
        raise TimeoutError
      rescue RuntimeError => err
        raise ProtocolError.new(err.message)
      end
    end
  end
end

Redis::Connection.drivers << Redis::Connection::Hiredis
