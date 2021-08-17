# PySpark витрина для "Crimes in Boston"
Витрина на PySpark для набора данных ["Crimes in Boston"](https://www.kaggle.com/AnalyzeBoston/crimes-in-boston).

Выходные данные записываются в parquet-файл по пути *OUTPUT_PATH* (см. ниже).

## Метрики

**crimes_total** - общее количество преступлений в этом районе
**crimes_monthly** - медиана числа преступлений в месяц в этом районе
**frequent_crime_types** - три самых частых crime_type за всю историю наблюдений в этом районе, объединенных через запятую с одним пробелом ", ", расположенных в порядке убывания частоты
**crime_type** - первая часть NAME из таблицы offense_codes, разбитого по разделителю "-" (например, если NAME "BURGLARY - COMMERICAL - ATTEMPT", то crime_type "BURGLARY")
**lat** - широта координаты района, расчитанная как среднее по всем широтам инцидентов
**lng** - долгота координаты района, расчитанная как среднее по всем долготам инцидентов

## Запуск
*spark-submit boston_crimes.py [--crimes_path CRIMES_PATH]
[--codes_path CODES_PATH] [--output_path OUTPUT_PATH]*

### Параметры

*CRIMES_PATH* - путь до CSV-файла с преступлениями (по умолчанию "./data/crime.csv")

*CODES_PATH* - путь до CSV-файла с кодами преступлений (по умолчанию "./data/offense_codes.csv")

*OUTPUT_PATH* - путь до parquet-файла (по умолчанию "./boston_crimes.parquet")  

## Требования
Python >= 3.6