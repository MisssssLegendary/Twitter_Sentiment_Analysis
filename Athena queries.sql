# 1. create the data table
CREATE EXTERNAL TABLE IF NOT EXISTS `big_data_project`.`data` (
  `id` string,
  `name` string,
  `username` string,
  `tweet` string,
  `followers_count` int,
  `location` string,
  `geo` string,
  `created_at` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ptb2-effy/demo/data.csv/'
TBLPROPERTIES ('classification' = 'csv');



# 2. creat the prediction table
CREATE EXTERNAL TABLE IF NOT EXISTS `big_data_project`.`prediction` (
  `label` int,
  `tweet` string,
  `tokens` array < string >,
  `filtered` array < string >,
  `cv` array < double >,
  `features` array < double >,
  `rawprediction` array < double >,
  `probability` array < double >,
  `prediction` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://ptb2-effy/demo/predictions.parquet/'
TBLPROPERTIES ('classification' = 'parquet');



# 3. create the words table
CREATE TABLE "big_data_project"."words" WITH (format = 'parquet') AS
select tweet,
  words
from prediction
  cross join unnest(filtered) as t(words); -- use unnest to explode the 'filtered' column which contains an array of tokens of the tweets with stop words removed.


  
# 4. create the table with cleaned location 
CREATE TABLE "big_data_project"."clean_location" WITH (format = 'TEXTFILE', field_delimiter = ',') AS with a as (
  SELECT *,
    regexp_replace(
      location,
      '["]+',
      ''-- remove the double quotation mark from location
    ) AS location_without_quotes
  FROM data
)
select *,
  case
    when location_without_quotes in (
      'Alabama',
      'Alaska',
      'Arizona',
      'Arkansas',
      'California',
      'Colorado',
      'Connecticut',
      'Delaware',
      'Florida',
      'Georgia',
      'Hawaii',
      'Idaho',
      'Illinois',
      'Indiana',
      'Iowa',
      'Kansas',
      'Kentucky',
      'Louisiana',
      'Maine',
      'Maryland',
      'Massachusetts',
      'Michigan',
      'Minnesota',
      'Mississippi',
      'Missouri',
      'Montana',
      'Nebraska',
      'Nevada',
      'New Hampshire',
      'New Jersey',
      'New Mexico',
      'New York',
      'North Carolina',
      'North Dakota',
      'Ohio',
      'Oklahoma',
      'Oregon',
      'Pennsylvania',
      'Rhode Island',
      'South Carolina',
      'South Dakota',
      'Tennessee',
      'Texas',
      'Utah',
      'Vermont',
      'Virginia',
      'Washington',
      'West Virginia',
      'Wisconsin',
      'Wyoming',
      'New York City',
      'New York',
      'Chicago',
      'Las Vegas',
      'Seattle',
      'San Francisco',
      'Washington',
      'D.C.',
      'New Orleans',
      'Palm Springs',
      'San Diego',
      'St. Louis',
      'Honolulu',
      'Miami',
      'Boston',
      'Orlando',
      'Nashville',
      'Los Angeles',
      'Dallas',
      'Austin',
      'Houston',
      'Atlanta',
      'Brooklyn',
      'Denver',
      'Detroit',
      'NYC',
      'Philadelphia',
      'USA',
      'the US',
      'America'
    ) then 'United States'
    when location_without_quotes in(
      'London',
      'England',
      'UK',
      'Glasgow',
      'Manchester',
      'Scotland'
    ) then 'United Kingdom'
    when location_without_quotes in(
      'Bandung',
      'DKI Jakarta',
      'indonesia',
      'Jawa Tengah',
      'Jawa Timur',
      'Jawa Barat',
      'Yogyakarta',
      'Jakarta Capital Region',
      'Tangerang',
      'Kota Surabaya',
      'Jakarta',
      'Bogor',
      'Bekasi',
      'Kota Bandung',
      'Sukabumi',
      'Jakarta Timur',
      'Lampung',
      'Depok',
      'Kalimantan Timur',
      '🇮🇩'
    ) then 'Indonesia'
    when location_without_quotes in (
      'Toronto',
      'Vancouver',
      'National Capital Region'
    ) then 'Canada'
    when location_without_quotes in ('Melbourne', 'Sydney') then 'Australia'
    when location_without_quotes in ('🇵🇭', 'Republic of the Philippines') then 'Philippines' else location_without_quotes
  end as location_clean-- pick the most common countries and map their provinces/cities to the countries
from a;

# 5. create the table that joins the data table and prediction table
CREATE TABLE "big_data_project"."clean_data" WITH (format = 'TEXTFILE', field_delimiter = ',') AS with a as (
  SELECT *,
    regexp_replace(
      location,
      '["]+',
      ''
    ) AS location_without_quotes
  FROM data
)
select a.id,
  a.name,
  a.username,
  a.tweet,
  a.followers_count, -- leave the columns with arrays and vectors
  case
    when p.label = 1 then 'positive'
    when p.label = 0 then 'negative'
  end as sentiment,
  case
    when cast(p.prediction as int) = 1 then 'positive' else 'negative'
  end as prediction, -- change '0' and '1' in label and prediction columns to 'negative' and 'positive' for easier interpretation
  case
    when a.location_without_quotes in (
      'Alabama',
      'Alaska',
      'Arizona',
      'Arkansas',
      'California',
      'Colorado',
      'Connecticut',
      'Delaware',
      'Florida',
      'Georgia',
      'Hawaii',
      'Idaho',
      'Illinois',
      'Indiana',
      'Iowa',
      'Kansas',
      'Kentucky',
      'Louisiana',
      'Maine',
      'Maryland',
      'Massachusetts',
      'Michigan',
      'Minnesota',
      'Mississippi',
      'Missouri',
      'Montana',
      'Nebraska',
      'Nevada',
      'New Hampshire',
      'New Jersey',
      'New Mexico',
      'New York',
      'North Carolina',
      'North Dakota',
      'Ohio',
      'Oklahoma',
      'Oregon',
      'Pennsylvania',
      'Rhode Island',
      'South Carolina',
      'South Dakota',
      'Tennessee',
      'Texas',
      'Utah',
      'Vermont',
      'Virginia',
      'Washington',
      'West Virginia',
      'Wisconsin',
      'Wyoming',
      'New York City',
      'New York',
      'Chicago',
      'Las Vegas',
      'Seattle',
      'San Francisco',
      'Washington',
      'D.C.',
      'New Orleans',
      'Palm Springs',
      'San Diego',
      'St. Louis',
      'Honolulu',
      'Miami',
      'Boston',
      'Orlando',
      'Nashville',
      'Los Angeles',
      'Dallas',
      'Austin',
      'Houston',
      'Atlanta',
      'Brooklyn',
      'Denver',
      'Detroit',
      'NYC',
      'Philadelphia',
      'USA',
      'the US',
      'America'
    ) then 'United States'
    when a.location_without_quotes in(
      'London',
      'England',
      'UK',
      'Glasgow',
      'Manchester',
      'Scotland'
    ) then 'United Kingdom'
    when a.location_without_quotes in(
      'Bandung',
      'DKI Jakarta',
      'indonesia',
      'Jawa Tengah',
      'Jawa Timur',
      'Jawa Barat',
      'Yogyakarta',
      'Jakarta Capital Region',
      'Tangerang',
      'Kota Surabaya',
      'Jakarta',
      'Bogor',
      'Bekasi',
      'Kota Bandung',
      'Sukabumi',
      'Jakarta Timur',
      'Lampung',
      'Depok',
      'Kalimantan Timur',
      '🇮🇩'
    ) then 'Indonesia'
    when a.location_without_quotes in (
      'Toronto',
      'Vancouver',
      'National Capital Region'
    ) then 'Canada'
    when a.location_without_quotes in ('Melbourne', 'Sydney') then 'Australia'
    when a.location_without_quotes in ('🇵🇭', 'Republic of the Philippines') then 'Philippines' else location_without_quotes
  end as location_clean
from a
  right join prediction p on a.tweet = p.tweet

