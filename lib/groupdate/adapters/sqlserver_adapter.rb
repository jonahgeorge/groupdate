module Groupdate
  module Adapters
    class SqlServerAdapter < BaseAdapter
      def group_clause
        time_zone = @time_zone.formatted_offset
        day_start_column = "DATEADD(SECOND, ?, SWITCHOFFSET(#{column}, ?))"
        day_start_interval = day_start * -1

        query =
          case period
          when :minute_of_hour
            ["CAST(DATEPART(MINUTE, #{day_start_column}) AS INT)", day_start_interval, time_zone]
          when :hour_of_day
            ["CAST(DATEPART(HOUR, #{day_start_column}) AS INT)", day_start_interval, time_zone]
          when :day_of_week
            ["CAST(DATEPART(WEEKDAY, #{day_start_column}) AS INT) - 1", day_start_interval, time_zone]
          when :day_of_month
            ["CAST(DAY(#{day_start_column}) AS INT)", day_start_interval, time_zone]
          when :day_of_year
            ["CAST(DATEPART(DAYOFYEAR, #{day_start_column}) AS INT)", day_start_interval, time_zone]
          when :month_of_year
            ["CAST(MONTH(#{day_start_column}) AS INT)", day_start_interval, time_zone]
          when :week
            ["CAST(DATETRUNC(WEEK, #{day_start_column}) AS DATE)", day_start_interval, time_zone]
          when :quarter
            ["CAST(DATETRUNC(QUARTER, #{day_start_column}) AS DATE)", day_start_interval, time_zone]
          when :day
            ["CAST(DATETRUNC(DAY, #{day_start_column}) AS DATE)", day_start_interval, time_zone]
          when :month
            ["CAST(DATETRUNC(MONTH, #{day_start_column}) AS DATE)", day_start_interval, time_zone]
          when :year
            ["CAST(DATETRUNC(YEAR, #{day_start_column}) AS DATE)", day_start_interval, time_zone]
          when :custom
            ["DATEADD(SECOND, CAST(LEFT(FLOOR(DATEDIFF(SECOND, '1970-01-01', #{column}) / ?) * ?, 10) AS INT), '1970-01-01')", n_seconds, n_seconds]
          when :second
            ["DATETRUNC(SECOND, #{day_start_column})", day_start_interval, time_zone]
          when :minute
            ["DATETRUNC(MINUTE, #{day_start_column})", day_start_interval, time_zone]
          when :hour
            ["DATETRUNC(HOUR, #{day_start_column})", day_start_interval, time_zone]
          else
            raise Groupdate::Error, "'#{period}' not supported for SQL Server"
          end

          @relation.send(:sanitize_sql_array, query)
      end
    end
  end
end
