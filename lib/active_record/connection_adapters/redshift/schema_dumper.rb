module ActiveRecord
  module ConnectionAdapters
    module Redshift
      module ColumnDumper
        # Adds +:array+ option to the default set provided by the
        # AbstractAdapter
        def prepare_column_options(column) # :nodoc:
          spec = super
          spec[:default] = "\"#{column.default_function}\"" if column.default_function
          spec
        end
      end
    end
  end

  class SchemaDumper
    def table(table, stream)
      columns = @connection.columns(table)
      begin
        tbl = StringIO.new

        # first dump primary key column
        pk = @connection.primary_key(table)

        tbl.print "  create_table #{remove_prefix_and_suffix(table).inspect}"
        pkcol = columns.detect { |c| c.name == pk }
        pk_buf = ''
        if pkcol
          if pkcol.sql_type == 'bigint'
            pk_buf << "t.primary_key :#{pk}, :bigint, null: false"
          elsif pkcol.sql_type == 'uuid'
            pk_buf << "t.primary_key :#{pk}, :uuid, null: false, default: \"#{pkcol.default_function.inspect}\""
          else
            pk_buf << "t.primary_key :#{pk}, :primary_key, null: false"
          end
        end
        tbl.print ", id: false"
        tbl.print ", force: :cascade"

        table_options = @connection.table_options(table)
        if table_options.present?
          tbl.print ", #{format_options(table_options)}"
        end

        tbl.puts " do |t|"

        if pk_buf
          tbl.puts "    " + pk_buf
        end

        # then dump all non-primary key columns
        columns.each do |column|
          raise StandardError, "Unknown type '#{column.sql_type}' for column '#{column.name}'" unless @connection.valid_type?(column.type)
          next if column.name == pk
          type, colspec = column_spec(column)
          tbl.print "    t.#{type} #{column.name.inspect}"
          tbl.print ", #{format_colspec(colspec)}" if colspec.present?
          tbl.print "#{format_encoding(column.encoding)}"
          tbl.puts
        end

        indexes_in_create(table, tbl)

        tbl.puts "  end"
        tbl.puts

        indexes(table, tbl)

        tbl.rewind
        stream.print tbl.read
      rescue => e
        stream.puts "# Could not dump table #{table.inspect} because of following #{e.class}"
        stream.puts "#   #{e.message}"
        stream.puts
      end

      stream
    end

    def format_encoding(encoding)
      if encoding.nil? or encoding == "none"
        return ""
      else
        ", encoding: \"#{encoding}\""
      end 
    end
  end
end
