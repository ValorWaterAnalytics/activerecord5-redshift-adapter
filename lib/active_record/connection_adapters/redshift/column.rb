module ActiveRecord
  module ConnectionAdapters
    class RedshiftColumn < Column #:nodoc:
      delegate :oid, :fmod, to: :sql_type_metadata

      def initialize(name, default, sql_type_metadata, null = true, table_name = nil, default_function = nil, encoding = nil)
        super name, default, sql_type_metadata, null, table_name, default_function, nil
        @null = null
        @default_function = default_function
        @encoding = encoding
      end

      def encoding
        @encoding
      end

      def null
        @null
      end
    end
  end
end
