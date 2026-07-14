from airflow.sdk import dag, task


@dag
def create_tracking_labels():

    @task
    def get_orders():
        return [
            {"order_id": "CM-2001", "destination": "Luna Depot", "status": "confirmed", "tracking_number": "LN-88213"},
            {"order_id": "CM-2002", "destination": "Mars Colony", "status": "cancelled"},
            {"order_id": "CM-2003", "destination": "Mars Colony", "status": "cancelled"},
            {"order_id": "CM-2004", "destination": "Ceres Outpost", "status": "confirmed", "tracking_number": "CR-40217"},
        ]

    @task
    def drop_cancelled(orders):
        for order in orders:
            if order["status"] == "cancelled":
                orders.remove(order)
        return orders

    @task
    def print_tracking_labels(orders):
        return [f"{o['order_id']}: {o['tracking_number']}" for o in orders]

    print_tracking_labels(drop_cancelled(get_orders()))


create_tracking_labels()