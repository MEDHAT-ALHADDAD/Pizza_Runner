# Case Study \#2 - Pizza Runner

## Introduction
Did you know that over 115 million kilograms of pizza is consumed daily worldwide??? (Well according to Wikipedia anyway…)

Danny was scrolling through his Instagram feed when something really caught his eye - “80s Retro Styling and Pizza Is The Future!”

Danny was sold on the idea, but he knew that pizza alone was not going to help him get seed funding to expand his new Pizza Empire - so he had one more genius idea to combine with it - he was going to Uberize it - and so Pizza Runner was launched!

Danny started by recruiting “runners” to deliver fresh pizza from Pizza Runner Headquarters (otherwise known as Danny’s house) and also maxed out his credit card to pay freelance developers to build a mobile app to accept orders from customers.

## Available Data
Because Danny had a few years of experience as a data scientist - he was very aware that data collection was going to be critical for his business’ growth.

He has prepared for us an entity relationship diagram of his database design but requires further assistance to clean his data and apply some basic calculations so he can better direct his runners and optimise Pizza Runner’s operations.

All datasets exist within the pizza_runner database schema - be sure to include this reference within your SQL scripts as you start exploring the data and answering the case study questions.

![ERD.png](./ERD.png)

---

### Table 1: runners
The runners table shows the registration_date for each new runner

|runner_id | registration_date |
| ----------- | ----------- |
|1 | 2021-01-01 |
|2 | 2021-01-03 |
|3 | 2021-01-08 |
|4 | 2021-01-15 |

### Table 2: customer_orders
Customer pizza orders are captured in the customer_orders table with 1 row for each individual pizza that is part of the order.

The pizza_id relates to the type of pizza which was ordered whilst the exclusions are the ingredient_id values which should be removed from the pizza and the extras are the ingredient_id values which need to be added to the pizza.

Note that customers can order multiple pizzas in a single order with varying exclusions and extras values even if the pizza is the same type!

The exclusions and extras columns will need to be cleaned up before using them in your queries.

| order_id |customer_id | pizza_id | exclusions | extras | order_time |
| ----------- | ----------- | ----------- | ----------- | ----------- | ----------- |
| 1 |101 | 1 |     | 	 | 2021-01-01 18:05:02 |
| 2 |101 | 1 |     | 	 | 2021-01-01 19:00:52 |
| 3 |102 | 1 |     |	 | 2021-01-02 23:51:23 |
| 3 |102 | 2 |     | NaN | 2021-01-02 23:51:23 |
| 4 |103 | 1 | 4   |     | 2021-01-04 13:23:46 |
| 4 |103 | 1 | 4   |     | 2021-01-04 13:23:46 |
| 4 |103 | 2 | 4   |     | 2021-01-04 13:23:46 |
| 5 |104 | 1 | null|1    | 2021-01-08 21:00:29 |
| 6 |101 | 2 | null|null | 2021-01-08 21:03:13 |
| 7 |105 | 2 | null|1	 | 2021-01-08 21:20:29 |
| 8 |102 | 1 | null|null | 2021-01-09 23:54:33 |
| 9 |103 | 1 | 4   |1, 5 | 2021-01-10 11:22:59 |
| 10 |104| 1 | null|null | 2021-01-11 18:34:49 |
| 10 |104| 1 | 2, 6|1, 4 | 2021-01-11 18:34:49 |

### Table 3: runner_orders
After each orders are received through the system - they are assigned to a runner - however not all orders are fully completed and can be cancelled by the restaurant or the customer.

The pickup_time is the timestamp at which the runner arrives at the Pizza Runner headquarters to pick up the freshly cooked pizzas. The distance and duration fields are related to how far and long the runner had to travel to deliver the order to the respective customer.

There are some known data issues with this table so be careful when using this in your queries - make sure to check the data types for each column in the schema SQL!

|order_id|	runner_id|	pickup_time|distance |duration |cancellation|
| ----------- | ----------- | ----------- | ----------- | ----------- | ----------- |
|1|	1|	2021-01-01 18:15:34	|20km|	32 minutes	| |
|2|	1|	2021-01-01 19:10:54	|20km|	27 minutes|	 |
|3|	1|	2021-01-03 00:12:37	|13.4km|	20 mins|	NaN|
|4|	2|	2021-01-04 13:53:03	|23.4|	40	|NaN|
|5|	3|	2021-01-08 21:10:57	|10|	15	|NaN|
|6|	3|	null|	null|	null|	Restaurant Cancellation|
|7|	2|	2020-01-08 21:30:45|	25km|	25mins|	null|
|8|	2|	2020-01-10 00:15:02|	23.4 km|	15 minute|	null|
|9|	2|	null|	null	|null	|Customer Cancellation|
|10|	1|	2020-01-11 18:50:20|	10km	|10minutes	|null|

### Table 4: pizza_names
At the moment - Pizza Runner only has 2 pizzas available the Meat Lovers or Vegetarian!

|pizza_id |	pizza_name |
| ----------- | ----------- |
|1 |	Meat Lovers |
|2 |	Vegetarian |

### Table 5: pizza_recipes
Each pizza_id has a standard set of toppings which are used as part of the pizza recipe.

|pizza_id	|toppings|
| ----------- | ----------- |
|1	        |1, 2, 3, 4, 5, 6, 8, 10|
|2	        |4, 6, 7, 9, 11, 12|

### Table 6: pizza_toppings
This table contains all of the topping_name values with their corresponding topping_id value

|topping_id|	topping_name|
| ----------- | ----------- |
|1|	Bacon|
|2|	BBQ Sauce|
|3|	Beef|
|4|	Cheese|
|5|	Chicken|
|6|	Mushrooms|
|7|	Onions|
|8|	Pepperoni|
|9|	Peppers|
|10|	Salami|
|11|	Tomatoes|
|12|	Tomato Sauce|


## PART I: Connecting to DB

### 1- Establish Connection


```python
%load_ext sql
from sqlalchemy import create_engine
conn_text = 'postgresql://{}:{}@{}/{}'.format(
    pg_user, pg_password, pg_host, pg_db
)

%sql $conn_text
engine = create_engine(conn_text)
```

 ### 2-Create DB


```sql
%%sql

CREATE SCHEMA IF NOT EXISTS pizza_runner;
SET search_path = pizza_runner;

DROP TABLE IF EXISTS runners;
CREATE TABLE runners (
  "runner_id" INTEGER,
  "registration_date" DATE
);
INSERT INTO runners
  ("runner_id", "registration_date")
VALUES
  (1, '2021-01-01'),
  (2, '2021-01-03'),
  (3, '2021-01-08'),
  (4, '2021-01-15');


DROP TABLE IF EXISTS customer_orders;
CREATE TABLE customer_orders (
  "order_id" INTEGER,
  "customer_id" INTEGER,
  "pizza_id" INTEGER,
  "exclusions" VARCHAR(4),
  "extras" VARCHAR(4),
  "order_time" TIMESTAMP
);

INSERT INTO customer_orders
  ("order_id", "customer_id", "pizza_id", "exclusions", "extras", "order_time")
VALUES
  ('1', '101', '1', '', '', '2020-01-01 18:05:02'),
  ('2', '101', '1', '', '', '2020-01-01 19:00:52'),
  ('3', '102', '1', '', '', '2020-01-02 23:51:23'),
  ('3', '102', '2', '', NULL, '2020-01-02 23:51:23'),
  ('4', '103', '1', '4', '', '2020-01-04 13:23:46'),
  ('4', '103', '1', '4', '', '2020-01-04 13:23:46'),
  ('4', '103', '2', '4', '', '2020-01-04 13:23:46'),
  ('5', '104', '1', 'null', '1', '2020-01-08 21:00:29'),
  ('6', '101', '2', 'null', 'null', '2020-01-08 21:03:13'),
  ('7', '105', '2', 'null', '1', '2020-01-08 21:20:29'),
  ('8', '102', '1', 'null', 'null', '2020-01-09 23:54:33'),
  ('9', '103', '1', '4', '1, 5', '2020-01-10 11:22:59'),
  ('10', '104', '1', 'null', 'null', '2020-01-11 18:34:49'),
  ('10', '104', '1', '2, 6', '1, 4', '2020-01-11 18:34:49');


DROP TABLE IF EXISTS runner_orders;
CREATE TABLE runner_orders (
  "order_id" INTEGER,
  "runner_id" INTEGER,
  "pickup_time" VARCHAR(19),
  "distance" VARCHAR(7),
  "duration" VARCHAR(10),
  "cancellation" VARCHAR(23)
);

INSERT INTO runner_orders
  ("order_id", "runner_id", "pickup_time", "distance", "duration", "cancellation")
VALUES
  ('1', '1', '2020-01-01 18:15:34', '20km', '32 minutes', ''),
  ('2', '1', '2020-01-01 19:10:54', '20km', '27 minutes', ''),
  ('3', '1', '2020-01-03 00:12:37', '13.4km', '20 mins', NULL),
  ('4', '2', '2020-01-04 13:53:03', '23.4', '40', NULL),
  ('5', '3', '2020-01-08 21:10:57', '10', '15', NULL),
  ('6', '3', 'null', 'null', 'null', 'Restaurant Cancellation'),
  ('7', '2', '2020-01-08 21:30:45', '25km', '25mins', 'null'),
  ('8', '2', '2020-01-10 00:15:02', '23.4 km', '15 minute', 'null'),
  ('9', '2', 'null', 'null', 'null', 'Customer Cancellation'),
  ('10', '1', '2020-01-11 18:50:20', '10km', '10minutes', 'null');


DROP TABLE IF EXISTS pizza_names;
CREATE TABLE pizza_names (
  "pizza_id" INTEGER,
  "pizza_name" TEXT
);
INSERT INTO pizza_names
  ("pizza_id", "pizza_name")
VALUES
  (1, 'Meatlovers'),
  (2, 'Vegetarian');


DROP TABLE IF EXISTS pizza_recipes;
CREATE TABLE pizza_recipes (
  "pizza_id" INTEGER,
  "toppings" TEXT
);
INSERT INTO pizza_recipes
  ("pizza_id", "toppings")
VALUES
  (1, '1, 2, 3, 4, 5, 6, 8, 10'),
  (2, '4, 6, 7, 9, 11, 12');


DROP TABLE IF EXISTS pizza_toppings;
CREATE TABLE pizza_toppings (
  "topping_id" INTEGER,
  "topping_name" TEXT
);
INSERT INTO pizza_toppings
  ("topping_id", "topping_name")
VALUES
  (1, 'Bacon'),
  (2, 'BBQ Sauce'),
  (3, 'Beef'),
  (4, 'Cheese'),
  (5, 'Chicken'),
  (6, 'Mushrooms'),
  (7, 'Onions'),
  (8, 'Pepperoni'),
  (9, 'Peppers'),
  (10, 'Salami'),
  (11, 'Tomatoes'),
  (12, 'Tomato Sauce');
```

     * postgresql://medhat:***@localhost/medhat
    Done.
    Done.
    Done.
    Done.
    4 rows affected.
    Done.
    Done.
    14 rows affected.
    Done.
    Done.
    10 rows affected.
    Done.
    Done.
    2 rows affected.
    Done.
    Done.
    2 rows affected.
    Done.
    Done.
    12 rows affected.





    []



### 3- Checking DB created successfully


```sql
%%sql

SELECT
    runners.runner_id,
    runners.registration_date,
    COUNT(DISTINCT runner_orders.order_id) AS orders
FROM pizza_runner.runners
INNER JOIN pizza_runner.runner_orders
    ON runners.runner_id = runner_orders.runner_id
WHERE runner_orders.cancellation IS NOT NULL
GROUP BY
    runners.runner_id,
    runners.registration_date;
```

     * postgresql://medhat:***@localhost/medhat
    3 rows affected.





<table>
    <thead>
        <tr>
            <th>runner_id</th>
            <th>registration_date</th>
            <th>orders</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>2021-01-01</td>
            <td>3</td>
        </tr>
        <tr>
            <td>2</td>
            <td>2021-01-03</td>
            <td>3</td>
        </tr>
        <tr>
            <td>3</td>
            <td>2021-01-08</td>
            <td>1</td>
        </tr>
    </tbody>
</table>



## PART II: Exploring DB

### 1- checking tables.columns and dtypes


```sql
%%sql

SELECT ordinal_position, table_name, column_name, data_type
FROM INFORMATION_SCHEMA.columns
WHERE table_schema = 'pizza_runner'
ORDER BY table_name, ordinal_position;
```

     * postgresql://medhat:***@localhost/medhat
    46 rows affected.





<table>
    <thead>
        <tr>
            <th>ordinal_position</th>
            <th>table_name</th>
            <th>column_name</th>
            <th>data_type</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>customer_orders</td>
            <td>order_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>2</td>
            <td>customer_orders</td>
            <td>customer_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>3</td>
            <td>customer_orders</td>
            <td>pizza_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>4</td>
            <td>customer_orders</td>
            <td>exclusions</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>5</td>
            <td>customer_orders</td>
            <td>extras</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>6</td>
            <td>customer_orders</td>
            <td>order_time</td>
            <td>timestamp without time zone</td>
        </tr>
        <tr>
            <td>1</td>
            <td>customer_orders_clean</td>
            <td>order_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>2</td>
            <td>customer_orders_clean</td>
            <td>customer_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>3</td>
            <td>customer_orders_clean</td>
            <td>pizza_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>4</td>
            <td>customer_orders_clean</td>
            <td>exclusions</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>5</td>
            <td>customer_orders_clean</td>
            <td>extras</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>6</td>
            <td>customer_orders_clean</td>
            <td>order_time</td>
            <td>timestamp without time zone</td>
        </tr>
        <tr>
            <td>1</td>
            <td>order_fact</td>
            <td>order_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>2</td>
            <td>order_fact</td>
            <td>customer_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>3</td>
            <td>order_fact</td>
            <td>pizza_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>4</td>
            <td>order_fact</td>
            <td>runner_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>5</td>
            <td>order_fact</td>
            <td>order_date</td>
            <td>timestamp without time zone</td>
        </tr>
        <tr>
            <td>6</td>
            <td>order_fact</td>
            <td>pickup_time</td>
            <td>timestamp without time zone</td>
        </tr>
        <tr>
            <td>7</td>
            <td>order_fact</td>
            <td>duration</td>
            <td>interval</td>
        </tr>
        <tr>
            <td>8</td>
            <td>order_fact</td>
            <td>expected_arrival</td>
            <td>timestamp without time zone</td>
        </tr>
        <tr>
            <td>9</td>
            <td>order_fact</td>
            <td>distance</td>
            <td>numeric</td>
        </tr>
        <tr>
            <td>10</td>
            <td>order_fact</td>
            <td>exclusions</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>11</td>
            <td>order_fact</td>
            <td>extras</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>12</td>
            <td>order_fact</td>
            <td>cancellation</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>1</td>
            <td>pizza_names</td>
            <td>pizza_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>2</td>
            <td>pizza_names</td>
            <td>pizza_name</td>
            <td>text</td>
        </tr>
        <tr>
            <td>1</td>
            <td>pizza_recipes</td>
            <td>pizza_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>2</td>
            <td>pizza_recipes</td>
            <td>toppings</td>
            <td>text</td>
        </tr>
        <tr>
            <td>1</td>
            <td>pizza_toppings</td>
            <td>topping_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>2</td>
            <td>pizza_toppings</td>
            <td>topping_name</td>
            <td>text</td>
        </tr>
        <tr>
            <td>1</td>
            <td>runner_orders</td>
            <td>order_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>2</td>
            <td>runner_orders</td>
            <td>runner_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>3</td>
            <td>runner_orders</td>
            <td>pickup_time</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>4</td>
            <td>runner_orders</td>
            <td>distance</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>5</td>
            <td>runner_orders</td>
            <td>duration</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>6</td>
            <td>runner_orders</td>
            <td>cancellation</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>1</td>
            <td>runner_orders_clean</td>
            <td>order_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>2</td>
            <td>runner_orders_clean</td>
            <td>runner_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>3</td>
            <td>runner_orders_clean</td>
            <td>pickup_time</td>
            <td>timestamp without time zone</td>
        </tr>
        <tr>
            <td>4</td>
            <td>runner_orders_clean</td>
            <td>distance</td>
            <td>numeric</td>
        </tr>
        <tr>
            <td>5</td>
            <td>runner_orders_clean</td>
            <td>duration</td>
            <td>interval</td>
        </tr>
        <tr>
            <td>6</td>
            <td>runner_orders_clean</td>
            <td>cancellation</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>1</td>
            <td>runners</td>
            <td>runner_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>2</td>
            <td>runners</td>
            <td>registration_date</td>
            <td>date</td>
        </tr>
        <tr>
            <td>1</td>
            <td>runners_clean</td>
            <td>runner_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>2</td>
            <td>runners_clean</td>
            <td>registration_date</td>
            <td>date</td>
        </tr>
    </tbody>
</table>




```sql
%%sql

-- explore missing and unique values for each table

SELECT 'exclusions: (' || string_agg(DISTINCT exclusions, ' | ') || ')' FROM customer_orders
UNION
SELECT 'extras: (' || string_agg(DISTINCT extras, ' | ') || ')' FROM customer_orders
UNION
SELECT 'pizza_name: (' || string_agg(DISTINCT pizza_name, ' | ') || ')' FROM pizza_names
UNION
SELECT 'pizza_recipes: (' || string_agg(DISTINCT toppings, ' | ') || ')' FROM pizza_recipes
UNION
SELECT 'cancellation: (' || string_agg(DISTINCT cancellation, ' | ') || ')' FROM runner_orders;
```

     * postgresql://medhat:***@localhost/medhat
    5 rows affected.





<table>
    <thead>
        <tr>
            <th>?column?</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>exclusions: ( | 2, 6 | 4 | null)</td>
        </tr>
        <tr>
            <td>cancellation: ( | Customer Cancellation | Restaurant Cancellation | null)</td>
        </tr>
        <tr>
            <td>pizza_recipes: (1, 2, 3, 4, 5, 6, 8, 10 | 4, 6, 7, 9, 11, 12)</td>
        </tr>
        <tr>
            <td>pizza_name: (Meatlovers | Vegetarian)</td>
        </tr>
        <tr>
            <td>extras: ( | 1 | 1, 4 | 1, 5 | null)</td>
        </tr>
    </tbody>
</table>



#### Notes
##### we have 6 tables, wrangling and cleaning requirments
. RELATIONS AND TYPES: 
 - [x] customer_orders(exclusions, extras) --> trim, replace null and empty with null
 - [x] runner_orders(pickup_time to timestamp & duration to interval, distance to int, cancellation nan, null, and empty to null)

. ADDED COLUMNS: 
 - [x] add expected_arrival column (runner_order)


### 2- Clean Tables

#### i) customer_orders


```sql
%%sql
DROP TABLE IF EXISTS customer_orders_clean;

CREATE TABLE customer_orders_clean AS (
    SELECT * FROM customer_orders
);

UPDATE customer_orders_clean
    SET exclusions= NULL WHERE exclusions IN ('null', '');
UPDATE customer_orders_clean
    SET extras= NULL WHERE extras IN ('null', '');
    

SELECT * FROM customer_orders_clean 
ORDER BY 1;
```

     * postgresql://medhat:***@localhost/medhat
    Done.
    14 rows affected.
    9 rows affected.
    9 rows affected.
    14 rows affected.





<table>
    <thead>
        <tr>
            <th>order_id</th>
            <th>customer_id</th>
            <th>pizza_id</th>
            <th>exclusions</th>
            <th>extras</th>
            <th>order_time</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>101</td>
            <td>1</td>
            <td>None</td>
            <td>None</td>
            <td>2020-01-01 18:05:02</td>
        </tr>
        <tr>
            <td>2</td>
            <td>101</td>
            <td>1</td>
            <td>None</td>
            <td>None</td>
            <td>2020-01-01 19:00:52</td>
        </tr>
        <tr>
            <td>3</td>
            <td>102</td>
            <td>1</td>
            <td>None</td>
            <td>None</td>
            <td>2020-01-02 23:51:23</td>
        </tr>
        <tr>
            <td>3</td>
            <td>102</td>
            <td>2</td>
            <td>None</td>
            <td>None</td>
            <td>2020-01-02 23:51:23</td>
        </tr>
        <tr>
            <td>4</td>
            <td>103</td>
            <td>1</td>
            <td>4</td>
            <td>None</td>
            <td>2020-01-04 13:23:46</td>
        </tr>
        <tr>
            <td>4</td>
            <td>103</td>
            <td>1</td>
            <td>4</td>
            <td>None</td>
            <td>2020-01-04 13:23:46</td>
        </tr>
        <tr>
            <td>4</td>
            <td>103</td>
            <td>2</td>
            <td>4</td>
            <td>None</td>
            <td>2020-01-04 13:23:46</td>
        </tr>
        <tr>
            <td>5</td>
            <td>104</td>
            <td>1</td>
            <td>None</td>
            <td>1</td>
            <td>2020-01-08 21:00:29</td>
        </tr>
        <tr>
            <td>6</td>
            <td>101</td>
            <td>2</td>
            <td>None</td>
            <td>None</td>
            <td>2020-01-08 21:03:13</td>
        </tr>
        <tr>
            <td>7</td>
            <td>105</td>
            <td>2</td>
            <td>None</td>
            <td>1</td>
            <td>2020-01-08 21:20:29</td>
        </tr>
        <tr>
            <td>8</td>
            <td>102</td>
            <td>1</td>
            <td>None</td>
            <td>None</td>
            <td>2020-01-09 23:54:33</td>
        </tr>
        <tr>
            <td>9</td>
            <td>103</td>
            <td>1</td>
            <td>4</td>
            <td>1, 5</td>
            <td>2020-01-10 11:22:59</td>
        </tr>
        <tr>
            <td>10</td>
            <td>104</td>
            <td>1</td>
            <td>2, 6</td>
            <td>1, 4</td>
            <td>2020-01-11 18:34:49</td>
        </tr>
        <tr>
            <td>10</td>
            <td>104</td>
            <td>1</td>
            <td>None</td>
            <td>None</td>
            <td>2020-01-11 18:34:49</td>
        </tr>
    </tbody>
</table>



#### ii) runner_orders


```sql
%%sql
DROP TABLE IF EXISTS runner_orders_clean;

CREATE TABLE IF NOT EXISTS runner_orders_clean AS (
    SELECT * FROM runner_orders
);

-- REPLACE 'null' with NULL
UPDATE runner_orders_clean
    SET pickup_time = NULL WHERE pickup_time = 'null';
UPDATE runner_orders_clean
    SET duration = NULL WHERE duration = 'null';
UPDATE runner_orders_clean
    SET distance = NULL WHERE distance = 'null';
UPDATE runner_orders_clean
    SET cancellation = NULL WHERE cancellation IN ('null', '');


UPDATE runner_orders_clean
    SET duration = REPLACE( REPLACE( REPLACE(
        TRIM(duration), 'minute', ''), 's', ''), 'min', '') || 'm',
    distance = REPLACE( TRIM(distance), 'km', '');
    

    
-- CAST COLUMNS
ALTER TABLE runner_orders_clean 
    ALTER COLUMN pickup_time TYPE TIMESTAMP USING pickup_time::timestamp without time zone,
    ALTER COLUMN distance TYPE NUMERIC USING distance::numeric,
    ALTER COLUMN duration TYPE INTERVAL USING duration::interval;
        
-- CHECK COLUMNS DTYPE
SELECT ordinal_position, table_name, column_name, data_type
FROM INFORMATION_SCHEMA.columns
WHERE table_name = 'runner_orders_clean';
```

     * postgresql://medhat:***@localhost/medhat
    Done.
    10 rows affected.
    2 rows affected.
    2 rows affected.
    2 rows affected.
    5 rows affected.
    10 rows affected.
    Done.
    6 rows affected.





<table>
    <thead>
        <tr>
            <th>ordinal_position</th>
            <th>table_name</th>
            <th>column_name</th>
            <th>data_type</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>runner_orders_clean</td>
            <td>order_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>2</td>
            <td>runner_orders_clean</td>
            <td>runner_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>3</td>
            <td>runner_orders_clean</td>
            <td>pickup_time</td>
            <td>timestamp without time zone</td>
        </tr>
        <tr>
            <td>4</td>
            <td>runner_orders_clean</td>
            <td>distance</td>
            <td>numeric</td>
        </tr>
        <tr>
            <td>5</td>
            <td>runner_orders_clean</td>
            <td>duration</td>
            <td>interval</td>
        </tr>
        <tr>
            <td>6</td>
            <td>runner_orders_clean</td>
            <td>cancellation</td>
            <td>character varying</td>
        </tr>
    </tbody>
</table>




```sql
%%sql
SELECT * FROM runner_orders_clean
```

     * postgresql://medhat:***@localhost/medhat
    10 rows affected.





<table>
    <thead>
        <tr>
            <th>order_id</th>
            <th>runner_id</th>
            <th>pickup_time</th>
            <th>distance</th>
            <th>duration</th>
            <th>cancellation</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>3</td>
            <td>1</td>
            <td>2020-01-03 00:12:37</td>
            <td>13.4</td>
            <td>0:20:00</td>
            <td>None</td>
        </tr>
        <tr>
            <td>4</td>
            <td>2</td>
            <td>2020-01-04 13:53:03</td>
            <td>23.4</td>
            <td>0:40:00</td>
            <td>None</td>
        </tr>
        <tr>
            <td>5</td>
            <td>3</td>
            <td>2020-01-08 21:10:57</td>
            <td>10</td>
            <td>0:15:00</td>
            <td>None</td>
        </tr>
        <tr>
            <td>6</td>
            <td>3</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>Restaurant Cancellation</td>
        </tr>
        <tr>
            <td>9</td>
            <td>2</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>Customer Cancellation</td>
        </tr>
        <tr>
            <td>1</td>
            <td>1</td>
            <td>2020-01-01 18:15:34</td>
            <td>20</td>
            <td>0:32:00</td>
            <td>None</td>
        </tr>
        <tr>
            <td>2</td>
            <td>1</td>
            <td>2020-01-01 19:10:54</td>
            <td>20</td>
            <td>0:27:00</td>
            <td>None</td>
        </tr>
        <tr>
            <td>7</td>
            <td>2</td>
            <td>2020-01-08 21:30:45</td>
            <td>25</td>
            <td>0:25:00</td>
            <td>None</td>
        </tr>
        <tr>
            <td>8</td>
            <td>2</td>
            <td>2020-01-10 00:15:02</td>
            <td>23.4</td>
            <td>0:15:00</td>
            <td>None</td>
        </tr>
        <tr>
            <td>10</td>
            <td>1</td>
            <td>2020-01-11 18:50:20</td>
            <td>10</td>
            <td>0:10:00</td>
            <td>None</td>
        </tr>
    </tbody>
</table>



## 3- Modeling

To simplify Queries, let's merge customer and runner orders in one order_fact table.

 - [X] order_fact [
     order_id,
     customer_id,
     runner_id,
     pizza_id,
     pickup_time,
     order_date,
     duration,
     expected_arrival,
     distance,
     exclusions,
     extras,
     cancellation]

### i) Create Order Fact Table


```sql
%%sql

-- CREATE order_fact
DROP TABLE IF EXISTS order_fact;

CREATE TABLE IF NOT EXISTS order_fact AS
    SELECT
        customer_orders_clean.order_id,
        customer_id,
        pizza_id,
        runner_id,
        order_time AS order_date,
        pickup_time,
        duration,
        pickup_time + duration AS expected_arrival,
        distance,
        exclusions,
        extras,
        cancellation
    FROM customer_orders_clean
    JOIN runner_orders_clean
    USING(order_id)
    ORDER BY 1;
    
SELECT *
FROM order_fact
LIMIT 3;
```

     * postgresql://medhat:***@localhost/medhat
    Done.
    14 rows affected.
    3 rows affected.





<table>
    <thead>
        <tr>
            <th>order_id</th>
            <th>customer_id</th>
            <th>pizza_id</th>
            <th>runner_id</th>
            <th>order_date</th>
            <th>pickup_time</th>
            <th>duration</th>
            <th>expected_arrival</th>
            <th>distance</th>
            <th>exclusions</th>
            <th>extras</th>
            <th>cancellation</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>101</td>
            <td>1</td>
            <td>1</td>
            <td>2020-01-01 18:05:02</td>
            <td>2020-01-01 18:15:34</td>
            <td>0:32:00</td>
            <td>2020-01-01 18:47:34</td>
            <td>20</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>2</td>
            <td>101</td>
            <td>1</td>
            <td>1</td>
            <td>2020-01-01 19:00:52</td>
            <td>2020-01-01 19:10:54</td>
            <td>0:27:00</td>
            <td>2020-01-01 19:37:54</td>
            <td>20</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>3</td>
            <td>102</td>
            <td>1</td>
            <td>1</td>
            <td>2020-01-02 23:51:23</td>
            <td>2020-01-03 00:12:37</td>
            <td>0:20:00</td>
            <td>2020-01-03 00:32:37</td>
            <td>13.4</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
    </tbody>
</table>



### ii) Create Date Dim


```sql
%%sql
DROP TABLE IF EXISTS date_dim;

CREATE TABLE IF NOT EXISTS date_dim AS
    SELECT 
        generate_series(min_date, max_date, '1 day'::INTERVAL) AS date
    FROM (
        SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date
        FROM order_fact
    ) AS date_intervals;
    
SELECT *
FROM date_dim
```

     * postgresql://medhat:***@localhost/medhat
    Done.
    11 rows affected.
    11 rows affected.





<table>
    <thead>
        <tr>
            <th>date</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2020-01-01 18:05:02</td>
        </tr>
        <tr>
            <td>2020-01-02 18:05:02</td>
        </tr>
        <tr>
            <td>2020-01-03 18:05:02</td>
        </tr>
        <tr>
            <td>2020-01-04 18:05:02</td>
        </tr>
        <tr>
            <td>2020-01-05 18:05:02</td>
        </tr>
        <tr>
            <td>2020-01-06 18:05:02</td>
        </tr>
        <tr>
            <td>2020-01-07 18:05:02</td>
        </tr>
        <tr>
            <td>2020-01-08 18:05:02</td>
        </tr>
        <tr>
            <td>2020-01-09 18:05:02</td>
        </tr>
        <tr>
            <td>2020-01-10 18:05:02</td>
        </tr>
        <tr>
            <td>2020-01-11 18:05:02</td>
        </tr>
    </tbody>
</table>



#### iii) spliting toppings to fixed-positional columns with concatenated desc, then denormalizing pizza_names, pizza_recipes, pizza_toppings in to single dim


```sql
%%sql

CREATE OR REPLACE VIEW pizza_r2 AS (
    SELECT 
        pizza_id,
        pizza_name,
        LOWER(REPLACE(STRING_AGG(topping_name, ','), ' ', '_')) AS toppings
    FROM pizza_names
    JOIN (
        SELECT
            pizza_id,
            UNNEST(REGEXP_MATCHES(toppings, '[0-9]{1,2}','g'))::INTEGER AS topping_id
        FROM pizza_recipes
    ) splitted_recipes
    USING(pizza_id)
    JOIN pizza_toppings
    USING(topping_id)
    GROUP BY 1, 2
);

SELECT * FROM pizza_r2;
```

     * postgresql://medhat:***@localhost/medhat
    Done.
    2 rows affected.





<table>
    <thead>
        <tr>
            <th>pizza_id</th>
            <th>pizza_name</th>
            <th>toppings</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>Meatlovers</td>
            <td>bbq_sauce,pepperoni,cheese,salami,chicken,bacon,mushrooms,beef</td>
        </tr>
        <tr>
            <td>2</td>
            <td>Vegetarian</td>
            <td>tomato_sauce,cheese,mushrooms,onions,peppers,tomatoes</td>
        </tr>
    </tbody>
</table>




```sql
%%sql

DROP TABLE IF EXISTS pizza_dim;

CREATE TABLE IF NOT EXISTS pizza_dim AS (
    SELECT pizza_id, pizza_name
    FROM pizza_names
);

-- 1) create toppings columns
DO $$
DECLARE
    col TEXT;

BEGIN
  -- Generate column dynamically
  FOR col IN
    SELECT toppings[i]
    FROM (
        SELECT
            string_to_array(
                STRING_AGG(topping_name, ','), ',') AS toppings,
            generate_series(1, 
                array_length(
                    string_to_array(
                        STRING_AGG(topping_name, ','), ','), 1)) AS i
        FROM pizza_toppings
    ) subquery
  LOOP
    EXECUTE 'ALTER TABLE pizza_dim ADD COLUMN ' || replace(col, ' ', '_') || ' BOOL';
  END LOOP;
        EXECUTE 'ALTER TABLE pizza_dim ADD COLUMN concat_toppings TEXT';
END $$;

SELECT * FROM pizza_dim;
```

     * postgresql://medhat:***@localhost/medhat
    Done.
    2 rows affected.
    Done.
    2 rows affected.





<table>
    <thead>
        <tr>
            <th>pizza_id</th>
            <th>pizza_name</th>
            <th>bacon</th>
            <th>bbq_sauce</th>
            <th>beef</th>
            <th>cheese</th>
            <th>chicken</th>
            <th>mushrooms</th>
            <th>onions</th>
            <th>pepperoni</th>
            <th>peppers</th>
            <th>salami</th>
            <th>tomatoes</th>
            <th>tomato_sauce</th>
            <th>concat_toppings</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>Meatlovers</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>2</td>
            <td>Vegetarian</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
    </tbody>
</table>




```sql
%%sql

-- FILL Boolean columns 
DO $$
DECLARE
    p_id INT;
    query_text TEXT;
    record_toppings TEXT;
    col_name TEXT;
    condition TEXT;
    column_names TEXT[] := (
        SELECT
            string_to_array(
                REPLACE(STRING_AGG(topping_name, ','), ' ', '_'), ',')
            AS toppings
        FROM pizza_toppings
    );
BEGIN
    -- Iterate over pizza_ids
    FOR p_id IN SELECT pizza_id FROM pizza_dim
    LOOP
        query_text := 'UPDATE pizza_dim SET ';

        -- Iterate over columns
        FOR col_name IN 
            SELECT col[i]
            FROM (
                SELECT column_names AS col,
                    generate_series(1, array_length(column_names, 1), 1) AS i
            ) subquery
        LOOP
            
            -- current pizza_toppings
            record_toppings := (
                SELECT toppings FROM pizza_r2 WHERE pizza_id=p_id
            );
            
            condition := CASE 
                WHEN record_toppings ILIKE '%' || col_name || '%' 
                THEN 'TRUE' 
                ELSE 'FALSE' END;
        
            query_text := query_text || col_name || '=' || condition || ', ';
        END LOOP;
        query_text := query_text || 'concat_toppings= '''|| record_toppings || '''';
        query_text := query_text || ' WHERE pizza_id = ' || p_id || ';';

        EXECUTE query_text;
    END LOOP;
END $$;

SELECT * FROM pizza_dim;
```

     * postgresql://medhat:***@localhost/medhat
    Done.
    2 rows affected.





<table>
    <thead>
        <tr>
            <th>pizza_id</th>
            <th>pizza_name</th>
            <th>bacon</th>
            <th>bbq_sauce</th>
            <th>beef</th>
            <th>cheese</th>
            <th>chicken</th>
            <th>mushrooms</th>
            <th>onions</th>
            <th>pepperoni</th>
            <th>peppers</th>
            <th>salami</th>
            <th>tomatoes</th>
            <th>tomato_sauce</th>
            <th>concat_toppings</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>Meatlovers</td>
            <td>True</td>
            <td>True</td>
            <td>True</td>
            <td>True</td>
            <td>True</td>
            <td>True</td>
            <td>False</td>
            <td>True</td>
            <td>False</td>
            <td>True</td>
            <td>False</td>
            <td>False</td>
            <td>bacon,bbq_sauce,beef,cheese,chicken,mushrooms,pepperoni,salami</td>
        </tr>
        <tr>
            <td>2</td>
            <td>Vegetarian</td>
            <td>False</td>
            <td>False</td>
            <td>False</td>
            <td>True</td>
            <td>False</td>
            <td>True</td>
            <td>True</td>
            <td>False</td>
            <td>True</td>
            <td>False</td>
            <td>True</td>
            <td>True</td>
            <td>cheese,mushrooms,onions,peppers,tomatoes,tomato_sauce</td>
        </tr>
    </tbody>
</table>



#### b) repeat steps for execlusions_inclusions_dim


```sql
%%sql
DROP TABLE IF EXISTS exclusions_extras_dim;

CREATE TABLE IF NOT EXISTS exclusions_extras_dim (
    order_id NUMERIC,
    pizza_id NUMERIC,
    bacon BOOL,
    bbq_sauce BOOL,
    beef BOOL,
    cheese BOOL,
    chicken BOOL,
    mushrooms BOOL,
    onions BOOL,
    pepperoni BOOL,
    peppers BOOL,
    salami BOOL,
    tomatoes BOOL,
    tomato_sauce BOOL,
    concat_toppings TEXT,
    exclusions_or_extras TEXT
);

INSERT INTO exclusions_extras_dim 
    (order_id, pizza_id, concat_toppings, exclusions_or_extras)
SELECT DISTINCT
    order_id,
    pizza_id,
    exclusions AS concat_toppings,
    'exclusions' AS exclusions_or_extras
FROM order_fact
WHERE exclusions IS NOT NULL
UNION
SELECT DISTINCT
    order_id,
    pizza_id,
    extras AS concat_toppings,
    'extras' AS exclusions_or_extras
FROM order_fact
WHERE extras IS NOT NULL
ORDER BY 1, 2;


SELECT * FROM exclusions_extras_dim;
```

     * postgresql://medhat:***@localhost/medhat
    Done.
    Done.
    8 rows affected.
    8 rows affected.





<table>
    <thead>
        <tr>
            <th>order_id</th>
            <th>pizza_id</th>
            <th>bacon</th>
            <th>bbq_sauce</th>
            <th>beef</th>
            <th>cheese</th>
            <th>chicken</th>
            <th>mushrooms</th>
            <th>onions</th>
            <th>pepperoni</th>
            <th>peppers</th>
            <th>salami</th>
            <th>tomatoes</th>
            <th>tomato_sauce</th>
            <th>concat_toppings</th>
            <th>exclusions_or_extras</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>4</td>
            <td>1</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>4</td>
            <td>exclusions</td>
        </tr>
        <tr>
            <td>4</td>
            <td>2</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>4</td>
            <td>exclusions</td>
        </tr>
        <tr>
            <td>5</td>
            <td>1</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>1</td>
            <td>extras</td>
        </tr>
        <tr>
            <td>7</td>
            <td>2</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>1</td>
            <td>extras</td>
        </tr>
        <tr>
            <td>9</td>
            <td>1</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>1, 5</td>
            <td>extras</td>
        </tr>
        <tr>
            <td>9</td>
            <td>1</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>4</td>
            <td>exclusions</td>
        </tr>
        <tr>
            <td>10</td>
            <td>1</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>2, 6</td>
            <td>exclusions</td>
        </tr>
        <tr>
            <td>10</td>
            <td>1</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>1, 4</td>
            <td>extras</td>
        </tr>
    </tbody>
</table>




```python

```

## PART III: Reporting

### 1- Pizza Metrics

#### i) How many pizzas were ordered?


```sql
%%sql

SELECT count(*)
FROM order_fact;
```

     * postgresql://medhat:***@localhost/medhat
    1 rows affected.





<table>
    <thead>
        <tr>
            <th>count</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>14</td>
        </tr>
    </tbody>
</table>



#### ii) How many unique customer orders were made?


```sql
%%sql

SELECT count(DISTINCT order_id)
FROM order_fact;
```

     * postgresql://medhat:***@localhost/medhat
    1 rows affected.





<table>
    <thead>
        <tr>
            <th>count</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>10</td>
        </tr>
    </tbody>
</table>



#### iii) How many successful orders were delivered by each runner?


```sql
%%sql

SELECT runner_id, COUNT(DISTINCT order_id) AS successful_orders
FROM order_fact
WHERE cancellation IS NULL
GROUP BY 1;
```

     * postgresql://medhat:***@localhost/medhat
    3 rows affected.





<table>
    <thead>
        <tr>
            <th>runner_id</th>
            <th>successful_orders</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>4</td>
        </tr>
        <tr>
            <td>2</td>
            <td>3</td>
        </tr>
        <tr>
            <td>3</td>
            <td>1</td>
        </tr>
    </tbody>
</table>



#### iv) How many of each type of pizza was delivered?


```sql
%%sql

SELECT pizza_name, count(*) AS delivered
FROM order_fact
JOIN pizza_names
USING(pizza_id)
WHERE cancellation IS NULL
GROUP BY 1
ORDER BY 2 DESC;
```

     * postgresql://medhat:***@localhost/medhat
    2 rows affected.





<table>
    <thead>
        <tr>
            <th>pizza_name</th>
            <th>delivered</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Meatlovers</td>
            <td>9</td>
        </tr>
        <tr>
            <td>Vegetarian</td>
            <td>3</td>
        </tr>
    </tbody>
</table>



#### v) How many Vegetarian and Meatlovers were ordered by each customer?


```sql
%%sql

SELECT 
    customer_id, 
    SUM(CASE WHEN pizza_id = 1 THEN 1 ELSE 0 END) AS meat_lovers_count,
    SUM(CASE WHEN pizza_id = 2 THEN 1 ELSE 0 END) AS vegetarian_count
FROM order_fact
JOIN pizza_names
USING(pizza_id)
GROUP BY 1
ORDER BY 1;
```

     * postgresql://medhat:***@localhost/medhat
    5 rows affected.





<table>
    <thead>
        <tr>
            <th>customer_id</th>
            <th>meat_lovers_count</th>
            <th>vegetarian_count</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>101</td>
            <td>2</td>
            <td>1</td>
        </tr>
        <tr>
            <td>102</td>
            <td>2</td>
            <td>1</td>
        </tr>
        <tr>
            <td>103</td>
            <td>3</td>
            <td>1</td>
        </tr>
        <tr>
            <td>104</td>
            <td>3</td>
            <td>0</td>
        </tr>
        <tr>
            <td>105</td>
            <td>0</td>
            <td>1</td>
        </tr>
    </tbody>
</table>



#### vi) What was the maximum number of pizzas delivered in a single order?


```sql
%%sql

SELECT order_id, count(pizza_id) AS max_pizzas_delivered
FROM order_fact
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;
```

     * postgresql://medhat:***@localhost/medhat
    1 rows affected.





<table>
    <thead>
        <tr>
            <th>order_id</th>
            <th>max_pizzas_delivered</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>4</td>
            <td>3</td>
        </tr>
    </tbody>
</table>



#### vii) For each customer, how many delivered pizzas had at least 1 change and how many had no changes?


```sql
%%sql

SELECT 
    customer_id,
    SUM(CASE WHEN exclusions IS NOT NULL OR extras IS NOT NULL
        THEN 1 ELSE 0 END) AS change_requested,
    SUM(CASE WHEN exclusions IS NULL AND extras IS NULL
        THEN 1 ELSE 0 END) AS no_change_requested
FROM order_fact
WHERE cancellation IS NULL
GROUP BY 1
ORDER BY 1;
```

     * postgresql://medhat:***@localhost/medhat
    5 rows affected.





<table>
    <thead>
        <tr>
            <th>customer_id</th>
            <th>change_requested</th>
            <th>no_change_requested</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>101</td>
            <td>0</td>
            <td>2</td>
        </tr>
        <tr>
            <td>102</td>
            <td>0</td>
            <td>3</td>
        </tr>
        <tr>
            <td>103</td>
            <td>3</td>
            <td>0</td>
        </tr>
        <tr>
            <td>104</td>
            <td>2</td>
            <td>1</td>
        </tr>
        <tr>
            <td>105</td>
            <td>1</td>
            <td>0</td>
        </tr>
    </tbody>
</table>



#### viii) How many pizzas were delivered that had both exclusions and extras?


```sql
%%sql

SELECT 
    SUM(CASE WHEN exclusions IS NOT NULL AND extras IS NOT NULL
        THEN 1 ELSE 0 END) AS special_pizzas
FROM order_fact
WHERE cancellation IS NULL
ORDER BY 1;
```

     * postgresql://medhat:***@localhost/medhat
    1 rows affected.





<table>
    <thead>
        <tr>
            <th>special_pizzas</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
        </tr>
    </tbody>
</table>



#### ix) What was the total volume of pizzas ordered for each hour of the day?


```sql
%%sql

-- for hours of the day
/* WITH hours_of_day AS ( SELECT generate_series(1, 24, 1) AS hod)

SELECT 
    h.hod,
    COALESCE(count, 0) AS orders
FROM hours_of_day as h
LEFT JOIN (
    SELECT 
        EXTRACT(hour FROM order_date) AS hod,
        Count(*)
    FROM order_fact
    GROUP BY 1
) AS order_per_hour
USING(hod)
ORDER BY 1; */

SELECT 
    EXTRACT(hour FROM order_date) AS hod,
    Count(*)
FROM order_fact
GROUP BY 1
ORDER BY 1;
```

     * postgresql://medhat:***@localhost/medhat
    6 rows affected.





<table>
    <thead>
        <tr>
            <th>hod</th>
            <th>count</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>11</td>
            <td>1</td>
        </tr>
        <tr>
            <td>13</td>
            <td>3</td>
        </tr>
        <tr>
            <td>18</td>
            <td>3</td>
        </tr>
        <tr>
            <td>19</td>
            <td>1</td>
        </tr>
        <tr>
            <td>21</td>
            <td>3</td>
        </tr>
        <tr>
            <td>23</td>
            <td>3</td>
        </tr>
    </tbody>
</table>



#### x) What was the volume of orders for each day of the week?


```sql
%%sql

SELECT 
    TO_CHAR(order_date, 'Day') AS day_of_week,
    Count(*) AS order_count
FROM order_fact
GROUP BY day_of_week, DATE_PART('dow', order_date)
ORDER BY DATE_PART('dow', order_date);
```

     * postgresql://medhat:***@localhost/medhat
    4 rows affected.





<table>
    <thead>
        <tr>
            <th>day_of_week</th>
            <th>order_count</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Wednesday</td>
            <td>5</td>
        </tr>
        <tr>
            <td>Thursday </td>
            <td>3</td>
        </tr>
        <tr>
            <td>Friday   </td>
            <td>1</td>
        </tr>
        <tr>
            <td>Saturday </td>
            <td>5</td>
        </tr>
    </tbody>
</table>



### 2- Runner and Customer Experience

#### i) How many runners signed up for each 1 week period? (i.e. week starts 2021-01-01)


```sql
%%sql

SELECT 
    (date_trunc('week', registration_date) + INTERVAL '4 DAYS')::DATE,
    count(*)    
FROM runners
GROUP BY 1
ORDER BY 1;
```

     * postgresql://medhat:***@localhost/medhat
    3 rows affected.





<table>
    <thead>
        <tr>
            <th>date</th>
            <th>count</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2021-01-01</td>
            <td>2</td>
        </tr>
        <tr>
            <td>2021-01-08</td>
            <td>1</td>
        </tr>
        <tr>
            <td>2021-01-15</td>
            <td>1</td>
        </tr>
    </tbody>
</table>



#### ii-a) What was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?


```sql
%%sql

SELECT 
    runner_id,
    EXTRACT( MINUTES FROM AVG(pickup_time - order_date)) AS AVG_MIN
FROM order_fact
GROUP BY 1
ORDER BY 1;
```

     * postgresql://medhat:***@localhost/medhat
    3 rows affected.





<table>
    <thead>
        <tr>
            <th>runner_id</th>
            <th>avg_min</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>15</td>
        </tr>
        <tr>
            <td>2</td>
            <td>23</td>
        </tr>
        <tr>
            <td>3</td>
            <td>10</td>
        </tr>
    </tbody>
</table>



#### ii-b) What is the average pick-up time in minutes  for the orders?


```sql
%%sql

WITH distinct_orders AS (
    SELECT DISTINCT
        order_id,
        EXTRACT( EPOCH FROM (pickup_time - order_date)) / 60 AS pickup_time
    FROM order_fact
)

SELECT round(avg(pickup_time), 2) AS avg_pickup_time
FROM distinct_orders
```

     * postgresql://medhat:***@localhost/medhat
    1 rows affected.





<table>
    <thead>
        <tr>
            <th>avg_pickup_time</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>15.98</td>
        </tr>
    </tbody>
</table>



#### iii) Is there any relationship between the number of pizzas and how long the order takes to prepare?


```sql
%%sql

-- assumption that pickup_time represent when the pizza is prepared

CREATE VIEW order_preparing_time AS (
    SELECT 
        order_id,
        count(*) AS pizza_numbers,
        MAX(pickup_time - order_date) AS preparing_time
    FROM order_fact
    GROUP BY 1
);

SELECT 
    pizza_numbers,
    round(avg(EXTRACT( EPOCH FROM preparing_time) / 60), 2) AS avg_prep_time
FROM order_preparing_time
GROUP BY 1
ORDER BY 1;
```

     * postgresql://medhat:***@localhost/medhat
    Done.
    3 rows affected.





<table>
    <thead>
        <tr>
            <th>pizza_numbers</th>
            <th>avg_prep_time</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>12.36</td>
        </tr>
        <tr>
            <td>2</td>
            <td>18.38</td>
        </tr>
        <tr>
            <td>3</td>
            <td>29.28</td>
        </tr>
    </tbody>
</table>




```sql
%%sql

WITH avg_prep_time AS (
    SELECT 
        pizza_numbers,
        round(avg(EXTRACT( EPOCH FROM preparing_time) / 60), 2) AS avg_prep_time
    FROM order_preparing_time
    GROUP BY 1
    ORDER BY 1
)

SELECT corr(pizza_numbers, avg_prep_time)
FROM avg_prep_time;
```

     * postgresql://medhat:***@localhost/medhat
    1 rows affected.





<table>
    <thead>
        <tr>
            <th>corr</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>0.986417838191981</td>
        </tr>
    </tbody>
</table>



>There appear to be a **strong corrolation** between number of pizzas in order and preparing time; on avarage every **new pizza** add about **10 min** to total_time.

#### iv) What was the average distance travelled for each customer?


```sql
%%sql

SELECT customer_id, ROUND(avg(distance), 2) AS avg_distance
FROM (
    SELECT DISTINCT order_id, customer_id, distance
    FROM order_fact
) AS uniq_orders
GROUP BY 1
ORDER BY 1;
```

     * postgresql://medhat:***@localhost/medhat
    5 rows affected.





<table>
    <thead>
        <tr>
            <th>customer_id</th>
            <th>avg_distance</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>101</td>
            <td>20.00</td>
        </tr>
        <tr>
            <td>102</td>
            <td>18.40</td>
        </tr>
        <tr>
            <td>103</td>
            <td>23.40</td>
        </tr>
        <tr>
            <td>104</td>
            <td>10.00</td>
        </tr>
        <tr>
            <td>105</td>
            <td>25.00</td>
        </tr>
    </tbody>
</table>



#### v) What was the difference between the longest and shortest delivery times for all orders?


```sql
%%sql

SELECT 
    DATE_part('min', MIN(duration)) AS "shortest_delivery (min)",
    DATE_part('min', MAX(duration)) AS "longest_delivery (min)",
    DATE_part('min', MAX(duration) - MIN(duration)) AS "duration_diff (min)"
FROM order_fact
```

     * postgresql://medhat:***@localhost/medhat
    1 rows affected.





<table>
    <thead>
        <tr>
            <th>shortest_delivery (min)</th>
            <th>longest_delivery (min)</th>
            <th>duration_diff (min)</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>10.0</td>
            <td>40.0</td>
            <td>30.0</td>
        </tr>
    </tbody>
</table>



#### vi) What was the average speed for each runner for each delivery and do you notice any trend for these values?


```sql
%%sql

CREATE OR REPLACE VIEW orders_speed AS (
    SELECT DISTINCT
        order_id,
        runner_id,
        distance,
        duration,
        round(
            distance / (EXTRACT(EPOCH FROM duration) / 60)
        , 2) AS "speed (km per min)"
    FROM order_fact
);

SELECT *
FROM orders_speed
ORDER BY 1;
```

     * postgresql://medhat:***@localhost/medhat
    Done.
    10 rows affected.





<table>
    <thead>
        <tr>
            <th>order_id</th>
            <th>runner_id</th>
            <th>distance</th>
            <th>duration</th>
            <th>speed (km per min)</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>1</td>
            <td>20</td>
            <td>0:32:00</td>
            <td>0.63</td>
        </tr>
        <tr>
            <td>2</td>
            <td>1</td>
            <td>20</td>
            <td>0:27:00</td>
            <td>0.74</td>
        </tr>
        <tr>
            <td>3</td>
            <td>1</td>
            <td>13.4</td>
            <td>0:20:00</td>
            <td>0.67</td>
        </tr>
        <tr>
            <td>4</td>
            <td>2</td>
            <td>23.4</td>
            <td>0:40:00</td>
            <td>0.59</td>
        </tr>
        <tr>
            <td>5</td>
            <td>3</td>
            <td>10</td>
            <td>0:15:00</td>
            <td>0.67</td>
        </tr>
        <tr>
            <td>6</td>
            <td>3</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>7</td>
            <td>2</td>
            <td>25</td>
            <td>0:25:00</td>
            <td>1.00</td>
        </tr>
        <tr>
            <td>8</td>
            <td>2</td>
            <td>23.4</td>
            <td>0:15:00</td>
            <td>1.56</td>
        </tr>
        <tr>
            <td>9</td>
            <td>2</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>10</td>
            <td>1</td>
            <td>10</td>
            <td>0:10:00</td>
            <td>1.00</td>
        </tr>
    </tbody>
</table>




```sql
%%sql

SELECT runner_id, round(avg("speed (km per min)"), 2)
FROM orders_speed
GROUP BY 1
ORDER BY 2 DESC;
```

     * postgresql://medhat:***@localhost/medhat
    3 rows affected.





<table>
    <thead>
        <tr>
            <th>runner_id</th>
            <th>round</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2</td>
            <td>1.05</td>
        </tr>
        <tr>
            <td>1</td>
            <td>0.76</td>
        </tr>
        <tr>
            <td>3</td>
            <td>0.67</td>
        </tr>
    </tbody>
</table>



with an avarage of 1 km/min runner 2 seems the fastest on delivering orders; without consedration to
- number of orders per each
- time in day of the order

#### vii) What is the successful delivery percentage for each runner?


```sql
%%sql
WITH distinct_orders AS (
    SELECT DISTINCT order_id, runner_id, cancellation
    FROM order_fact
    ORDER BY 1
), runner_orders_val AS (
    SELECT
        runner_id,
        SUM(CASE WHEN cancellation IS NULL THEN 1 ELSE 0 END) 
            AS successful_orders,
        count(order_id) AS total_orders
    FROM distinct_orders
    GROUP BY 1
)


SELECT *, 
    ROUND(successful_orders * 1.0 / total_orders, 2) AS successful_ratio
FROM runner_orders_val
ORDER BY 1;

```

     * postgresql://medhat:***@localhost/medhat
    3 rows affected.





<table>
    <thead>
        <tr>
            <th>runner_id</th>
            <th>successful_orders</th>
            <th>total_orders</th>
            <th>successful_ratio</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>4</td>
            <td>4</td>
            <td>1.00</td>
        </tr>
        <tr>
            <td>2</td>
            <td>3</td>
            <td>4</td>
            <td>0.75</td>
        </tr>
        <tr>
            <td>3</td>
            <td>1</td>
            <td>2</td>
            <td>0.50</td>
        </tr>
    </tbody>
</table>



### 3- Ingredient Optimisation

#### i) What are the standard ingredients for each pizza?


```sql
%%sql

SELECT 
    topping_id,
    topping_name
FROM pizza_toppings
WHERE topping_id IN (
    SELECT toppings
    FROM pizza_recipes
    WHERE pizza_id=1
)
```

     * postgresql://medhat:***@localhost/medhat
    (psycopg2.errors.UndefinedFunction) operator does not exist: integer = text
    LINE 5: WHERE topping_id IN (
                             ^
    HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.
    
    [SQL: SELECT 
        topping_id,
        topping_name
    FROM pizza_toppings
    WHERE topping_id IN (
        SELECT toppings
        FROM pizza_recipes
        WHERE pizza_id=1
    )]
    (Background on this error at: https://sqlalche.me/e/14/f405)


#### ii) What was the most commonly added extra?


```python
%%sql
```

#### iii) What was the most common exclusion?


```python
%%sql
```

#### iv) Generate an order item for each record in the customers_orders table in the format of one of the following:
Meat Lovers
Meat Lovers - Exclude Beef
Meat Lovers - Extra Bacon
Meat Lovers - Exclude Cheese, Bacon - Extra Mushroom, Peppers


```python
%%sql
```

#### v) Generate an alphabetically ordered comma separated ingredient list for each pizza order from the customer_orders table and add a 2x in front of any relevant ingredients
For example: "Meat Lovers: 2xBacon, Beef, ... , Salami"


```python
%%sql
```

    UsageError: %%sql is a cell magic, but the cell body is empty. Did you mean the line magic %sql (single %)?


#### vi) What is the total quantity of each ingredient used in all delivered pizzas sorted by most frequent first?


```python
%%sql
```

### 4- Pricing and Ratings

### 5- Bonus DML Challenges (DML = Data Manipulation Language)


```python

```


```python

```


```python

```


```python

```
