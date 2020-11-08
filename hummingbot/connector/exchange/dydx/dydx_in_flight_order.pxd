from hummingbot.connector.in_flight_order_base cimport InFlightOrderBase

cdef class DydxInFlightOrder(InFlightOrderBase):
    cdef:
        public object market
        public object status
        public long long created_at
        public str reserved_asset
        public set fills

        object _last_executed_amount_from_order_status
        list _queued_events
        list _queued_fill_events
