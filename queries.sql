TUGAS 1
-- Stage 1:
-- 	- Assign primary keys and foreign keys

-- First, I created all the tables and their columns along with datatype.

CREATE TABLE customers_dataset  (
	customer_id VARCHAR,
	customer_unique_id VARCHAR,
	customer_zip_code_prefix INTEGER,
	customer_city VARCHAR,
	customer_state VARCHAR
);

CREATE TABLE geolocation_dataset (
	geolocation_zip_code_prefix INTEGER,
	geolocation_lat DOUBLE PRECISION,
	geolocation_lng DOUBLE PRECISION,
	geolocation_city VARCHAR,
	geolocation_state VARCHAR
);

CREATE TABLE order_items_dataset (
	order_id VARCHAR,
	order_item_id INTEGER,
	product_id VARCHAR,
	seller_id VARCHAR,
	shipping_limit_date TIMESTAMP WITHOUT TIME ZONE,
	price DOUBLE PRECISION,
	freight_value DOUBLE PRECISION
);

CREATE TABLE order_payments_dataset (
	order_id VARCHAR,
	payment_sequential INTEGER,
	payment_type VARCHAR,
	payment_installments INTEGER,
	payment_value DOUBLE PRECISION
);

CREATE TABLE order_reviews_dataset (
	review_id VARCHAR,
	order_id VARCHAR,
	review_score INTEGER,
	review_comment_title VARCHAR,
	review_comment_message VARCHAR,
	review_creation_date TIMESTAMP WITHOUT TIME ZONE,
	review_answer_timestamp TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE orders_dataset (
	order_id VARCHAR,
	customer_id VARCHAR,
	order_status VARCHAR,
	order_purchase_timestamp TIMESTAMP WITHOUT TIME ZONE,
	order_approved_at TIMESTAMP WITHOUT TIME ZONE,
	order_delivered_carrier_date TIMESTAMP WITHOUT TIME ZONE,
	order_delivered_customer_date TIMESTAMP WITHOUT TIME ZONE,
	order_estimated_delivery_date TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE product_dataset (
	idx SERIAL,
	product_id VARCHAR,
	product_category_name VARCHAR,
	product_name_lenght DOUBLE PRECISION,
	product_description_lenght DOUBLE PRECISION,
	product_photos_qty DOUBLE PRECISION,
	product_weight_g DOUBLE PRECISION,
	product_length_cm DOUBLE PRECISION,
	product_height_cm DOUBLE PRECISION,
	product_width_cm DOUBLE PRECISION
);

CREATE TABLE sellers_dataset (
	seller_id VARCHAR,
	seller_zip_code_prefix INTEGER,
	seller_city VARCHAR,
	seller_state VARCHAR
);

-- The second step is importing csv to the corresponding columns.

-- After that, I assigned some columns as primary keys, using alter statement.

ALTER TABLE customers_dataset
ADD PRIMARY KEY (customer_id);

ALTER TABLE sellers_dataset
ADD PRIMARY KEY (seller_id);

ALTER TABLE orders_dataset
ADD PRIMARY KEY (order_id);

ALTER TABLE product_dataset
ADD PRIMARY KEY (product_id);

-- ALTER TABLE geolocation_dataset
-- ADD PRIMARY KEY (geolocation_zip_code_prefix);

-- Lastly, I added foreign keys to columns in reference to other columns.

-- ALTER TABLE customers_dataset
-- ADD FOREIGN KEY (customer_zip_code_prefix)
-- REFERENCES geolocation_dataset (geolocation_zip_code_prefix);

ALTER TABLE order_items_dataset
ADD FOREIGN KEY (order_id)
REFERENCES orders_dataset (order_id);

ALTER TABLE order_items_dataset
ADD FOREIGN KEY (product_id)
REFERENCES product_dataset (product_id);

ALTER TABLE order_items_dataset
ADD CONSTRAINT order_items_fk_3
FOREIGN KEY (seller_id)
REFERENCES sellers_dataset (seller_id);

ALTER TABLE order_payments_dataset
ADD FOREIGN KEY (order_id)
REFERENCES orders_dataset (order_id);

-- ALTER TABLE sellers_dataset
-- ADD FOREIGN KEY (seller_zip_code_prefix)
-- REFERENCES geolocation_dataset (geolocation_zip_code_prefix);

ALTER TABLE order_reviews_dataset
ADD FOREIGN KEY (order_id)
REFERENCES orders_dataset (order_id);

ALTER TABLE orders_dataset
ADD FOREIGN KEY (customer_id)
REFERENCES customers_dataset (customer_id);

-- You noticed some lines are scripted.
-- This is due to the invalid dataset, namely "geolocation_dataset".
-- This dataset has duplicated values across all its columns.
-- For that reason, the dataset is only imported.
-- No primary and foreign keys related to this particular table queried.



TUGAS 2

-- Stage 2:
-- 1. Average of monthly active users (MAU)
-- 2. Number of new users
-- 3. Number of users with repeat order
-- 4. Average number of orders

-- These are how each one of those is queried:

CREATE TABLE stage_2 (
    year INT,
	mau_avg DOUBLE PRECISION,
	customers_new INT,
	customers_repeat_order INT,
	customers_order_average INT
	);


-- 1. Average of monthly active users (MUA):

INSERT INTO 
    stage_2 (mau_avg, year) -- Insert results into table step_2
	SELECT
		AVG(customer_count) AS customer_count_avg, -- Average MAU
		purchase_year -- Only select the year
		FROM(
		SELECT
			COUNT(DISTINCT cd.customer_unique_id) AS customer_count, -- MAU
			DATE_PART ('month', od.order_purchase_timestamp) AS purchase_month, -- Month
			DATE_PART ('year', od.order_purchase_timestamp) AS purchase_year -- Year
			FROM orders_dataset od
			JOIN customers_dataset cd
			ON od.customer_id = cd.customer_id
			GROUP BY purchase_year, purchase_month -- Group by month and year
		) AS t1
		GROUP BY purchase_year; -- MUA averaged by year

	
-- 2. Number of new users:

UPDATE stage_2
SET customers_new = order_count_sum
FROM(
	SELECT
	purchase_year_first,
	COUNT(1)
	FROM (
		SELECT
		DISTINCT cd.customer_unique_id, -- Unique users
		DATE_PART ('year', MIN(od.order_purchase_timestamp)) AS purchase_year_first
		FROM orders_dataset od
		JOIN customers_dataset cd
		ON od.customer_id = cd.customer_id
		GROUP BY cd.customer_unique_id -- How many order(s) each user made
	) AS t1
	GROUP BY purchase_year_first
	ORDER BY purchase_year_first

	
-- 3. Number of users with repeat order:

UPDATE stage_2
SET customers_repeat_order = order_count_sum
	FROM(
		SELECT
			COUNT(order_count) AS order_count_sum, -- Total the number of orders
			purchase_year -- Year
			FROM (
				SELECT
					DISTINCT customer_unique_id AS unique_id, -- Unique users
					COUNT(order_id) AS order_count, -- Number of orders
					DATE_PART('year', order_purchase_timestamp) AS purchase_year
					FROM orders_dataset AS od
					JOIN customers_dataset AS cd
						ON od.customer_id = cd.customer_id
					GROUP BY unique_id, purchase_year -- How many order(s) each user made in each year
				) AS t1
			WHERE order_count > 1 -- How many users with two or more orders
			GROUP BY purchase_year -- How many of these users per year
		) t2
		WHERE stage_2.year = t2.purchase_year;


-- 4. Average number of orders:

UPDATE stage_2
SET customers_order_average = order_count_sum
	FROM(
		SELECT
			AVG(order_count) AS order_count_sum, -- Average number of orders per customer
			purchase_year
			FROM(
			SELECT
				DISTINCT customer_unique_id AS unique_id,
				COUNT(order_id) AS order_count, -- Number of orders
				DATE_PART('year', order_purchase_timestamp) AS purchase_year -- Year
				FROM orders_dataset AS od
				JOIN customers_dataset AS cd
					ON od.customer_id = cd.customer_id
				GROUP BY purchase_year, unique_id -- Number of orders in each year
				) t1
			GROUP BY purchase_year -- Average number of orders per customer per year
		) t2
	WHERE stage_2.year = t2.purchase_year;

SELECT *
	FROM stage_2

TUGAS 3

-- Stage 3:
-- 1. Annual revenue
-- 2. Number of cancelled orders per year
-- 3. Product category with the highest revenue per year
-- 4. kProduct category with the most cancelled orders per year


CREATE TABLE stage_3 ( -- create table with the name 'stage_3' to contain future analysis results
	year INT, -- year
	revenue DOUBLE PRECISION, -- annual revenue
	order_cancelled INT, -- number of cancelled orders per year
	revenue_highest_product_category VARCHAR, -- product category with the highest revenue in the year
	order_cancelled_highest_product_category VARCHAR) -- product category with the most canceled order in the year


-- 1.

INSERT INTO stage_3 (revenue, year) -- insert values to the columns in assigned table
SELECT
	SUM(price + freight_value) AS revenue, -- sum revenue
	purchase_year
FROM (
	SELECT
		price, -- item price
		freight_value, -- cost of shipping
		DATE_PART ('year', od.order_purchase_timestamp) AS purchase_year -- year
	FROM order_items_dataset AS oid
	JOIN orders_dataset AS od
		ON oid.order_id = od.order_id
	WHERE od.order_status != 'canceled' -- filter for all orders but canceled ones
	) AS t1
GROUP BY t1.purchase_year -- aggregate based on year
ORDER BY t1.purchase_year -- make sure the year is in logical order


-- 2.

UPDATE stage_3
	SET order_cancelled = canceled_count -- update assigned table
FROM (
	SELECT
		COUNT(*) AS canceled_count, -- count canceled count
		DATE_PART('year', od.order_purchase_timestamp) AS purchase_year -- year
	FROM orders_dataset AS od
	WHERE od.order_status = 'canceled'
	GROUP BY purchase_year -- aggregate based on year
	) AS t1
WHERE stage_3.year = t1.purchase_year


-- 3.

UPDATE stage_3
	SET revenue_highest_product_category = product_category_name -- update assigned table
FROM (
	SELECT
		purchase_year,
		product_category_name,
		rank -- third, show only the highest ranked
	FROM (
		SELECT
			purchase_year,
			product_category_name,
			rank() OVER (PARTITION BY purchase_year ORDER BY revenue DESC) -- second, rank annual revenue
		FROM (
			SELECT
				DATE_PART('year', order_purchase_timestamp) AS purchase_year, -- year
				product_category_name, -- product category
				(price + freight_value) AS revenue -- first, create new column revenue
			FROM orders_dataset AS od
			JOIN order_items_dataset AS oid
				ON od.order_id = oid.order_id
			JOIN product_dataset AS pd
				ON oid.product_id = pd.product_id
			) AS t1
		) AS t2
	WHERE rank = 1 -- filter for only the highest rank of revenue
	GROUP BY purchase_year, product_category_name, rank -- group based on year and product category respectively
	) AS t3
WHERE stage_3.year = t3.purchase_year


-- 4.

UPDATE stage_3
	SET order_cancelled_highest_product_category = product_category_name -- update assigned table
FROM (
	SELECT
		purchase_year,
		product_category_name,
		rank -- third, show only the highest ranked
	FROM (
		SELECT
			purchase_year,
			product_category_name,
			rank() OVER (PARTITION BY purchase_year ORDER BY order_canceled_count DESC) -- second, rank counted canceled order
		FROM (
			SELECT
				DATE_PART('year', order_purchase_timestamp) AS purchase_year, -- year
				product_category_name, -- product category
				COUNT(order_status) AS order_canceled_count -- first, count canceled order
			FROM orders_dataset AS od
			JOIN order_items_dataset AS oid
				ON od.order_id = oid.order_id
			JOIN product_dataset AS pd
				ON oid.product_id = pd.product_id
			WHERE order_status = 'canceled'
			GROUP BY purchase_year, product_category_name -- aggregate based on year and product category
			) AS t1
		) AS t2
	WHERE rank = 1 -- filter for only the highest rank of counted canceled order
	GROUP BY purchase_year, product_category_name, rank -- group based on year and product category respectively
	) AS t3
WHERE stage_3.year = t3.purchase_year


TUGAS 4

-- Stage 4:
-- 	- Usage Frequency Per Payment Type
-- 	- Usage Frequency Per Payment Type Per Year


CREATE TABLE stage_4 (
	payment_type VARCHAR,
	payment_type_frequency INT,
	year_2016 INT,
	year_2017 INT,
	year_2018 INT);
	

-- 1. Usage Frequency Per Payment Type

INSERT INTO stage_4 (payment_type, payment_type_frequency) -- insert the values into the respective table
SELECT
	payment_type,
	SUM(payment_type_count) AS payment_type_sum -- sum to get how many times a payment type was used
FROM (
	SELECT
		payment_type, -- payment type
		CASE payment_type -- assign 1 for summing up later
			WHEN 'not_defined' THEN 1
			WHEN 'boleto' THEN 1
			WHEN 'debit_card' THEN 1
			WHEN 'voucher' THEN 1
			WHEN 'credit_card' THEN 1
		END AS payment_type_count
	FROM order_payments_dataset
	) AS t1
GROUP BY payment_type -- aggregate based on payment type
ORDER BY payment_type_sum DESC; -- set in logical order


-- 2. Usage Frequency Per Payment Type Per Year

-- Year 2016

UPDATE stage_4 -- insert the values into the respective table
	SET
		year_2016 = payment_type_2016_sum
FROM (
	SELECT
		payment_type,
		SUM(payment_type_2016) AS payment_type_2016_sum -- sum to get how many times a payment type was used
	FROM (
		SELECT
			purchase_year,
			payment_type,
			CASE -- assign 1 for summing up later
				WHEN payment_type = 'not_defined' THEN 1
				WHEN payment_type = 'boleto' THEN 1
				WHEN payment_type = 'debit_card' THEN 1
				WHEN payment_type = 'voucher' THEN 1
				WHEN payment_type = 'credit_card' THEN 1
			END AS payment_type_2016
		FROM (
				SELECT
					DATE_PART ('year', order_purchase_timestamp) AS purchase_year, -- year
					payment_type -- payment type
				FROM orders_dataset AS od
				JOIN order_payments_dataset AS opd
					ON od.order_id = opd.order_id
			) AS t1
		WHERE purchase_year = 2016 -- filter for year 2016 only
		) AS t2
	GROUP BY payment_type -- aggregate to get the sum based on payment type
	) AS t3
WHERE stage_4.payment_type = t3.payment_type;


-- Year 2017

UPDATE stage_4 -- insert the values into the respective table
	SET
		year_2017 = payment_type_2017_sum
FROM (
	SELECT
		payment_type,
		SUM(payment_type_2017) AS payment_type_2017_sum -- sum to get how many times a payment type was used
	FROM (
		SELECT
			purchase_year,
			payment_type,
			CASE -- assign 1 for summing up later
				WHEN payment_type = 'not_defined' THEN 1
				WHEN payment_type = 'boleto' THEN 1
				WHEN payment_type = 'debit_card' THEN 1
				WHEN payment_type = 'voucher' THEN 1
				WHEN payment_type = 'credit_card' THEN 1
			END AS payment_type_2017
		FROM (
				SELECT
					DATE_PART ('year', order_purchase_timestamp) AS purchase_year, -- year
					payment_type -- payment type
				FROM orders_dataset AS od
				JOIN order_payments_dataset AS opd
					ON od.order_id = opd.order_id
			) AS t1
		WHERE purchase_year = 2017 -- filter for year 2017 only
		) AS t2
	GROUP BY payment_type -- aggregate to get the sum based on payment type
	) AS t3
WHERE stage_4.payment_type = t3.payment_type;


-- Year 2018

UPDATE stage_4 -- insert the values into the respective table
	SET
		year_2018 = payment_type_2018_sum
FROM (
	SELECT
		payment_type,
		SUM(payment_type_2018) AS payment_type_2018_sum -- sum to get how many times a payment type was used
	FROM (
		SELECT
			purchase_year,
			payment_type,
			CASE -- assign 1 for summing up later
				WHEN payment_type = 'not_defined' THEN 1
				WHEN payment_type = 'boleto' THEN 1
				WHEN payment_type = 'debit_card' THEN 1
				WHEN payment_type = 'voucher' THEN 1
				WHEN payment_type = 'credit_card' THEN 1
			END AS payment_type_2018
		FROM (
				SELECT
					DATE_PART ('year', order_purchase_timestamp) AS purchase_year, -- year
					payment_type -- payment type
				FROM orders_dataset AS od
				JOIN order_payments_dataset AS opd
					ON od.order_id = opd.order_id
			) AS t1
		WHERE purchase_year = 2018 -- filter for year 2018 only
		) AS t2
	GROUP BY payment_type -- aggregate to get the sum based on payment type
	) AS t3
WHERE stage_4.payment_type = t3.payment_type;
