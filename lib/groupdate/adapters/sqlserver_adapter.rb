module Groupdate
  module Adapters
    class SqlServerAdapter < BaseAdapter
      def group_clause
        raise Groupdate::Error, "Time zones not supported for SQL Server" unless @time_zone.utc_offset.zero?
        raise Groupdate::Error, "day_start not supported for SQL Server" unless day_start.zero?

        query =
          case period
          when :minute_of_hour
            ["DATEPART(MINUTE, #{column})"]
          when :hour_of_day
            ["DATEPART(HOUR, #{column})"]
          when :day_of_week
            ["DATEPART(WEEKDAY, #{column})"]
          when :day_of_month
            ["DATEPART(DAY, #{column})"]
          when :day_of_year
            ["DATEPART(DAYOFYEAR, #{column})"]
          when :month_of_year
            ["DATEPART(MONTH, #{column})"]
          when :week
            ["DATEADD(WEEK, DATEDIFF(WEEK, 0, #{column}), 0)"]
          when :quarter
            ["DATEADD(QUARTER, DATEDIFF(QUARTER, 0, #{column}), 0)"]
          when :day
            ["DATEADD(DAY, DATEDIFF(DAY, 0, #{column}), 0)"]
          when :month
            ["DATEADD(MONTH, DATEDIFF(MONTH, 0, #{column}), 0)"]
          when :year
            ["DATEADD(YEAR, DATEDIFF(YEAR, 0, #{column}), 0)"]
          when :second
            # ["DATEADD(SECOND, DATEDIFF(SECOND, 0, #{column}), 0)"]

            # Raises the following error:
            # The datediff function resulted in an overflow. The number of dateparts separating two date/time instances is too large.
            # Try to use datediff with a less precise datepart. (TinyTds::Error)
            raise Groupdate::Error, "Second not supported for SQLServer"
          when :minute
            ["DATEADD(MINUTE, DATEDIFF(MINUTE, 0, #{column}), 0)"]
          when :hour
            ["DATEADD(HOUR, DATEDIFF(HOUR, 0, #{column}), 0)"]
          else
            raise Groupdate::Error, "'#{period}' not supported for SQL Server"
          end

        @relation.send(:sanitize_sql_array, query)
      end
    end
  end
end
