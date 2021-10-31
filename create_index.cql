USE cs4224_keyspace;

CREATE INDEX IF NOT EXISTS customer_order_by_customer ON cs4224_keyspace.Customer_Order ("O_C_ID");
CREATE INDEX IF NOT EXISTS order_line_by_item ON cs4224_keyspace.Order_Line ("OL_I_ID");
