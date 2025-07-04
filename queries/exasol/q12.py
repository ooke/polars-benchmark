from queries.exasol import utils

Q_NUM = 12


def q() -> None:

    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()
    query_str = f"""
    select
            l_shipmode,
            sum(case
                when o_orderpriority = '1-URGENT'
                    or o_orderpriority = '2-HIGH'
                    then 1
                else 0
            end) as high_line_count,
            sum(case
                when o_orderpriority <> '1-URGENT'
                    and o_orderpriority <> '2-HIGH'
                    then 1
                else 0
            end) as low_line_count
        from
            {orders_ds},
            {line_item_ds}
        where
            o_orderkey = l_orderkey
            and l_shipmode in ('MAIL', 'SHIP')
            and l_commitdate < l_receiptdate
            and l_shipdate < l_commitdate
            and l_receiptdate >= date '1994-01-01'
            and l_receiptdate < date '1994-01-01' + interval '1' year
        group by
            l_shipmode
        order by
            l_shipmode
        ;
    """
    utils.run_query(Q_NUM, query_str)


if __name__ == "__main__":
    q()

