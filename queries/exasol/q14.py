from queries.exasol import utils

Q_NUM = 14


def q() -> None:

    line_item_ds = utils.get_line_item_ds()
    part_ds = utils.get_part_ds()
    query_str = f"""
    select
            round(100.00 * sum(case
                when p_type like 'PROMO%'
                    then l_extendedprice * (1 - l_discount)
                else 0
            end) / sum(l_extendedprice * (1 - l_discount)), 2) as promo_revenue
        from
            {line_item_ds},
            {part_ds}
        where
            l_partkey = p_partkey
            and l_shipdate >= date '1995-09-01'
            and l_shipdate < date '1995-09-01' + interval '1' month
        ;
    """
    utils.run_query(Q_NUM, query_str)


if __name__ == "__main__":
    q()

