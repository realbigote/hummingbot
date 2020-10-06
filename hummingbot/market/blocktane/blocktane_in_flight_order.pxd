from hummingbot.market.in_flight_order_base cimport InFlightOrderBase

cdef class BlocktaneInFlightOrder(InFlightOrderBase):
    cdef:
        public object trade_id_set
        public float created_at
