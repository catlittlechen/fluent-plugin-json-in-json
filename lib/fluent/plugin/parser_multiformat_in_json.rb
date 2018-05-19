require 'yajl'

module Fluent
  class TextParser
    class MultiFormatParser < Parser
      Fluent::Plugin.register_parser('mutilformat', self)

      config_param :time_key, :string, :default => 'time'
      config_param :time_format, :string, :default => nil
      config_param :json_pre, :string, :default => '{'
      config_param :ltsv_pre, :string, :default => 'time="'

      config_param :ltsv_delimiter, :string, default: "\t"
      config_param :ltsv_label_delimiter, :string, default: ":"

      config_param :default_key, :string, :default => 'service"'
      config_param :default_value, :string, :default => 'k8s"'

      def configure(conf)
        super

        unless @time_format.nil?
          @time_parser = TimeParser.new(@time_format)
          @mutex = Mutex.new
        end
      end

      def parse(text)
        record = Yajl.load(text)

        value = @keep_time_key ? record[@time_key] : record.delete(@time_key)
        if value
          if @time_format
            time = @mutex.synchronize { @time_parser.parse(value) }
          else
            begin
              time = value.to_i
            rescue => e
              raise ParserError, "invalid time value: value = #{value}, error_class = #{e.class.name}, error = #{e.message}"
            end
          end
        else
          if @estimate_current_event
            time = Engine.now
          else
            time = nil
          end
        end

        values = Hash.new
        record.each do |k, v|
          if v[0] == @json_pre
            deserialized = Yajl.load(v)
            if deserialized.is_a?(Hash)
              values.merge!(deserialized)
              record.delete k
            end
          else
            if v.start_with?(@ltsv_pre)
              v.split(@delimiter).each do |pair|
                key, value = pair.split(@label_delimiter, 2)
                if key == 'srv'
                  key = 'service'
                end
                values[key] = value
                record.delete k
              end
            end
          end
        end

        if values.has_key?("time")
          values.delete "time"
        end

        if not values.has_key?(@default_key)
          values[@default_key] = @default_value
        end

        values.each do |k, v|
          if not v
            values.delete k
          end
        end

        record.merge!(values)

        if block_given?
          yield time, record
        else
          return time, record
        end
      rescue Yajl::ParseError
        if block_given?
          yield nil, nil
        else
          return nil, nil
        end
      end
    end
  end
end
