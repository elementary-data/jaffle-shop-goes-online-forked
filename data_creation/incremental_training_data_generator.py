from datetime import datetime, timedelta
import os
import random
from collections import defaultdict
import hashlib
from utils.csv import (
    split_csv_to_headers_and_data,
    write_to_csv,
)

CURRENT_DIRECTORY_PATH = os.path.dirname(os.path.realpath(__file__))
ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME = "original_jaffle_shop_data"
NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH = (
    "../jaffle_shop_online/seeds/training"
)
NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH = (
    "../jaffle_shop_online/seeds/validation"
)

CUSTOMERS_COUNT = 2000
ORDERS_COUNT = 10000
TIME_SPAN_IN_DAYS = 30
LOWEST_PAYMENT_IN_HUNDRENDS = 0
HIGHEST_PAYMENT_IN_HUNDRENDS = 28
MAX_PAYMENTS_PER_ORDER = 2


def generate_incremental_training_data(data_creation_date):
    ammount_of_new_customers = generate_incremental_customers_data(data_creation_date)
    ammount_of_new_orders = generate_incremental_orders_data(data_creation_date)
    generate_incremental_payments_data(data_creation_date, ammount_of_new_orders)
    generate_incremental_signups_data(data_creation_date, ammount_of_new_customers)


def generate_incremental_customers_data(data_creation_date, ammount_of_new_data=None):
    ammount_of_new_data = (
        ammount_of_new_data
        if ammount_of_new_data
        else random.randint(
            int(CUSTOMERS_COUNT / TIME_SPAN_IN_DAYS - 50),
            int(CUSTOMERS_COUNT / TIME_SPAN_IN_DAYS),
        )
    )
    origin_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME, "raw_customers.csv"
    )
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_customers_training.csv",
    )
    origin_headers, origin_data = split_csv_to_headers_and_data(
        csv_path=origin_data_path
    )
    headers, current_customers_data = split_csv_to_headers_and_data(
        csv_path=new_data_path
    )
    all_first_names = list(set([row[1] for row in origin_data]))
    all_last_names = list(set([row[2] for row in origin_data]))
    new_customers_data = [*current_customers_data]
    for customer_id in range(
        len(current_customers_data) + 1,
        ammount_of_new_data + len(current_customers_data) + 1,
    ):
        new_customers_data.append(
            [
                customer_id,  # CUSTOMER ID
                all_first_names[
                    random.randint(0, len(all_first_names) - 1)
                ],  # FIRST NAME
                all_last_names[random.randint(0, len(all_last_names) - 1)],  # LAST NAME
            ]
        )
    write_to_csv(new_data_path, origin_headers, new_customers_data)
    return ammount_of_new_data


def generate_incremental_orders_data(data_creation_date, ammount_of_new_data=None):
    ammount_of_new_data = (
        ammount_of_new_data
        if ammount_of_new_data
        else random.randint(
            int(ORDERS_COUNT / TIME_SPAN_IN_DAYS - 50),
            int(ORDERS_COUNT / TIME_SPAN_IN_DAYS),
        )
    )
    customers_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_customers_training.csv",
    )
    origin_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME, "raw_orders.csv"
    )
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_orders_training.csv",
    )
    origin_headers, origin_data = split_csv_to_headers_and_data(
        csv_path=origin_data_path
    )
    headers, current_orders_data = split_csv_to_headers_and_data(csv_path=new_data_path)
    customers_headers, current_customers_data = split_csv_to_headers_and_data(
        csv_path=customers_data_path
    )
    all_order_statuses = list(set([row[3] for row in origin_data]))
    new_orders_data = [*current_orders_data]
    for order_id in range(
        len(current_orders_data) + 1, ammount_of_new_data + len(current_orders_data) + 1
    ):
        new_orders_data.append(
            [
                order_id,  # ORDER ID
                random.randint(1, len(current_customers_data)),  # CUSTOMER ID
                data_creation_date.strftime("%Y-%m-%d 02:00:00"),  # ORDER DATE
                all_order_statuses[
                    random.randint(0, len(all_order_statuses) - 1)
                ],  # ORDER STATUS
            ]
        )
    write_to_csv(new_data_path, origin_headers, new_orders_data)
    return ammount_of_new_data


def generate_incremental_payments_data(data_creation_date, ammount_of_new_orders):
    origin_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME, "raw_payments.csv"
    )
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_payments_training.csv",
    )
    orders_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_orders_training.csv",
    )
    origin_headers, origin_data = split_csv_to_headers_and_data(
        csv_path=origin_data_path
    )
    headers, current_payments_data = split_csv_to_headers_and_data(
        csv_path=new_data_path
    )
    cyrrent_orders_headers, current_orders_data = split_csv_to_headers_and_data(
        csv_path=orders_data_path
    )
    all_payments_methods = list(set([row[2] for row in origin_data]))
    new_payments_data = [*current_payments_data]
    payment_id = len(current_payments_data) + 1
    for order_id in range(
        len(current_orders_data) - ammount_of_new_orders + 1,
        len(current_orders_data) + 1,
    ):
        amount_of_payments = random.randint(1, MAX_PAYMENTS_PER_ORDER)
        max_total_payment_in_hundrends = HIGHEST_PAYMENT_IN_HUNDRENDS
        for payment in range(amount_of_payments):
            payment_amount_in_hundrends = random.randint(
                LOWEST_PAYMENT_IN_HUNDRENDS, max_total_payment_in_hundrends
            )
            new_payments_data.append(
                [
                    payment_id,  # PAYMENT_ID
                    order_id,  # ORDER ID
                    all_payments_methods[
                        random.randint(0, len(all_payments_methods) - 1)
                    ],  # PAYMENT METHOD
                    (payment_amount_in_hundrends + 1) * 100,  # AMOUNT
                ]
            )
            payment_id += 1
            max_total_payment_in_hundrends -= payment_amount_in_hundrends
    write_to_csv(new_data_path, origin_headers, new_payments_data)


def generate_incremental_signups_data(data_creation_date, ammount_of_new_customers):
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_signups_training.csv",
    )
    headers = ["id", "user_id", "user_email", "hashed_password", "signup_date"]
    customers_csv_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_customers_training.csv",
    )
    orders_csv_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_orders_training.csv",
    )
    signups_headers, signups_data = split_csv_to_headers_and_data(
        csv_path=new_data_path
    )
    customers_headers, customers_data = split_csv_to_headers_and_data(
        csv_path=customers_csv_path
    )
    orders_headers, orders_data = split_csv_to_headers_and_data(
        csv_path=orders_csv_path
    )

    customer_min_order_time_map = defaultdict(
        lambda: (
            datetime.now() - timedelta(random.randint(2, TIME_SPAN_IN_DAYS))
        ).strftime("%Y-%m-%d 02:00:00")
    )
    for order in orders_data:
        customer_min_order_time_map[order[1]] = min(
            datetime.strptime(
                customer_min_order_time_map[order[1]], "%Y-%m-%d %H:%M:%S"
            ),
            datetime.strptime(order[2], "%Y-%m-%d %H:%M:%S"),
        ).strftime("%Y-%m-%d %H:%M:%S")

    new_signups_data = [*signups_data]
    for customer in customers_data[len(customers_data) - ammount_of_new_customers :]:
        new_signups_data.append(
            [
                customer[0],  # SIGNUP ID
                customer[0],  # CUSTOMER ID
                f"{customer[1]}{customer[2].lower()}{customer[0]}@example.com"
                if random.randint(0, 30)
                else "",  # USER EMAIL
                hashlib.sha256(datetime.now().isoformat().encode()).hexdigest(),
                customer_min_order_time_map[customer[0]],
            ]
        )
    write_to_csv(new_data_path, headers, new_signups_data)
