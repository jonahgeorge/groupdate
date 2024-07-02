module Groupdate
  module Adapters
    class SqlServerAdapter < BaseAdapter
      def group_clause
        raise Groupdate::Error, "Time zones not supported for SQLServer" unless @time_zone.utc_offset.zero?
        raise Groupdate::Error, "day_start not supported for SQLServer" unless day_start.zero?

        query =
          case period
          when :minute_of_hour
            ["CAST(DATEPART(MINUTE, #{column}) AS INT)"]
          when :hour_of_day
            ["CAST(DATEPART(HOUR, #{column}) AS INT)"]
          when :day_of_week
            ["CAST((DATEPART(WEEKDAY, #{column}) + @@DATEFIRST - 1) %% 7 AS INT)"]
          when :day_of_month
            ["CAST(DATEPART(DAY, #{column}) AS INT)"]
          when :day_of_year
            ["CAST(DATEPART(DAYOFYEAR, #{column}) AS INT)"]
          when :month_of_year
            ["CAST(DATEPART(MONTH, #{column}) AS INT)"]
          when :week
            ["CAST(DATEADD(DAY, -((DATEPART(WEEKDAY, #{column}) - 1 + @@DATEFIRST - ? + 7) % 7), #{column}) AS DATE)", week_start + 1]
          when :quarter
            raise Groupdate::Error, "Quarter not supported for SQLServer"
          when :day
            ["CAST(DATETRUNC(DAY, #{column}) AS DATE)"]
          when :month
            ["CAST(DATETRUNC(MONTH, #{column}) AS DATE)"]
          when :year
            ["CAST(DATETRUNC(YEAR, #{column}) AS DATE)"]
          when :custom
            ["DATEADD(SECOND, CAST(LEFT(FLOOR(DATEDIFF(SECOND, '1970-01-01', #{column}) / ?) * ?, 10) AS INT), '1970-01-01')", n_seconds, n_seconds]
          when :second
            ["DATETRUNC(SECOND, #{column})"]
          when :minute
            ["DATETRUNC(MINUTE, #{column})"]
          when :hour
            ["DATETRUNC(HOUR, #{column})"]
          else
            raise Groupdate::Error, "'#{period}' not supported for SQL Server"
          end

          @relation.send(:sanitize_sql_array, query)
      end
    end
  end
end
