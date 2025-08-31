// 1.
select product_name,
    to_char(sum(quantity * unit_price), '$999999,990.00') as total_sales,
    -- Ranks del producto
    dense_rank() over (order by sum(quantity) desc) as ranking
from product_information pi, order_items oi, orders o
where pi.product_id = oi.product_id and oi.order_id = o.order_id
group by product_name;

// 2.
select category_name, product_name,
    to_char(sum(quantity * unit_price), '$999999,990.00') as total_sales,
    -- Ranks del producto
    dense_rank() over (partition by category_name order by sum(quantity * unit_price) desc) as category_rank,
    dense_rank() over (order by sum(quantity * unit_price) desc) as general_rank
from product_information pi, order_items oi, orders o, product_categories pc
where pi.product_id = oi.product_id
    and oi.order_id = o.order_id
    and pi.category_id = pc.category_id
group by category_name, product_name
order by category_name, category_rank;

// 3.
with category_family as (
    select pc.category_id, pc.category_name, fam.category_name as family_name
    from product_categories pc, product_categories fam
    where pc.family_id = fam.category_id
)
select family_name, category_name, product_name,
    to_char(sum(quantity * unit_price), '$999999,990.00') as total_sales,
    -- Ranks del producto
    dense_rank() over (partition by family_name order by sum(quantity * unit_price) desc) as family_rank,
    dense_rank() over (partition by category_name order by sum(quantity * unit_price) desc) as category_rank,
    dense_rank() over (order by sum(quantity * unit_price) desc) as general_rank
from product_information pi, order_items oi, orders o, category_family cf
where pi.product_id = oi.product_id
    and oi.order_id = o.order_id
    and pi.category_id = cf.category_id
group by family_name, category_name, product_name
order by category_name, family_rank;

// 4.
with category_family as (
    select pc.category_id, pc.category_name, fam.category_name as family_name
    from product_categories pc, product_categories fam
    where pc.family_id = fam.category_id
)
select family_name, category_name, product_name,
    to_char(sum(quantity * unit_price), '$999999,990.00') as total_sales,
    -- Ranks del producto
    dense_rank() over (partition by family_name order by sum(quantity * unit_price) desc) as family_rank,
    dense_rank() over (partition by category_name order by sum(quantity * unit_price) desc) as category_rank,
    dense_rank() over (order by sum(quantity * unit_price) desc) as general_rank,
     -- Porcentaje de las ventas del producto
    round((ratio_to_report (sum(quantity * unit_price)) over ()) * 100, 2) as general_pct,
    round((ratio_to_report (sum(quantity * unit_price)) over (partition by family_name)) * 100, 2) as family_pct,
    round((ratio_to_report (sum(quantity * unit_price)) over (partition by category_name)) * 100, 2) as category_pct
from product_information pi, order_items oi, orders o, category_family cf
where pi.product_id = oi.product_id
    and oi.order_id = o.order_id
    and pi.category_id = cf.category_id
group by family_name, category_name, product_name
order by category_name, family_rank;

// 5.
with report_top as (
    select country_name, state_province, product_name,
        dense_rank() over (partition by state_province order by sum(quantity * unit_price) desc) as ranking,
        sum(quantity * unit_price) as total_sales
    from product_information pi, orders o, order_items oi, countries c, customers cu
    where pi.product_id = oi.product_id
        and oi.order_id = o.order_id
        and o.customer_id = cu.customer_id
        and cu.country_id = c.country_id
    group by country_name, state_province, product_name
    order by state_province, total_sales desc
)
select country_name, state_province, product_name, ranking,
    to_char(total_sales, '$99999,990.00') as total_sales
from report_top
where ranking <= 3
order by state_province, total_sales desc;

// 6.
with report_top as (
    select country_name, state_province, product_name,
        dense_rank() over (partition by state_province order by sum(quantity * unit_price) asc) as ranking,
        sum(quantity * unit_price) as total_sales
    from product_information pi, orders o, order_items oi, countries c, customers cu
    where pi.product_id = oi.product_id
        and oi.order_id = o.order_id
        and o.customer_id = cu.customer_id
        and cu.country_id = c.country_id
    group by country_name, state_province, product_name
    order by state_province, total_sales desc
)
select country_name, state_province, product_name, ranking,
    to_char(total_sales, '$99999,990.00') as total_sales
from report_top
where ranking <= 3
order by state_province, total_sales asc;

// 7.
with report_bottom as (
    select country_name, state_province, product_name,
        dense_rank() over (partition by state_province order by sum(quantity * unit_price) desc) as top_ranking,
        dense_rank() over (partition by state_province order by sum(quantity * unit_price) asc) as bottom_ranking,
        sum(quantity * unit_price) as total_sales
    from product_information pi, orders o, order_items oi, countries c, customers cu
    where pi.product_id = oi.product_id
        and oi.order_id = o.order_id
        and o.customer_id = cu.customer_id
        and cu.country_id = c.country_id
    group by country_name, state_province, product_name
    order by state_province, total_sales desc
)
-- TOP
select country_name, state_province, product_name, top_ranking,
    to_char(total_sales, '$99999,990.00') as total_sales,
    'TOP ' || top_ranking as rank_pos
from report_bottom
where top_ranking <= 3
-- UNION
union all
-- BOTTOM
select country_name, state_province, product_name, bottom_ranking,
    to_char(total_sales, '$99999,990.00') as total_sales,
    'BOTTOM ' || bottom_ranking as rank_pos
from report_bottom
where bottom_ranking <= 3
order by state_province, rank_pos;

// 8.
with report_top_bottom as (
    select country_name, state_province, product_name,
        dense_rank() over (partition by state_province order by sum(quantity * unit_price) desc) as top_ranking,
        dense_rank() over (partition by state_province order by sum(quantity * unit_price) asc) as bottom_ranking,
        sum(quantity * unit_price) as total_sales
    from product_information pi, orders o, order_items oi, countries c, customers cu
    where pi.product_id = oi.product_id
        and oi.order_id = o.order_id
        and o.customer_id = cu.customer_id
        and cu.country_id = c.country_id
    group by country_name, state_province, product_name
    order by state_province, total_sales desc
)
-- TOP
select country_name, state_province, product_name, top_ranking,
    to_char(total_sales, '$99999,990.00') as total_sales,
    'TOP ' || top_ranking as rank_pos,
    sum(total_sales) over (partition by state_province) as total_sum,
    round(avg(total_sales) over (partition by state_province), 2) as total_avg
from report_top_bottom
where top_ranking <= 3
-- UNION
union all
-- BOTTOM
select country_name, state_province, product_name, bottom_ranking,
    to_char(total_sales, '$99999,990.00') as total_sales,
    'BOTTOM ' || bottom_ranking as rank_pos,
    sum(total_sales) over (partition by state_province) as total_sum,
    round(avg(total_sales) over (partition by state_province), 2) as total_avg
from report_top_bottom
where bottom_ranking <= 3
order by state_province, rank_pos;

// 9.
select trunc(order_date, 'Month') as month,
    to_char(sum(quantity * unit_price), '$99,999,999,999,990.00') as total_sales,
    round(avg(sum(quantity * unit_price))
        over (partition by state_province
        order by trunc(order_date, 'Month')
        range between interval '2' month preceding and current row), 2) as moving_avg
from customers cu, orders o, order_items oi, product_information pi
where pi.product_id = oi.product_id
    and oi.order_id = o.order_id
    and o.customer_id = cu.customer_id
    and lower(state_province) = 'wi'
group by trunc(order_date, 'Month'), state_province
order by month;

// 10.
with date_bounds as (
    -- Obtener las fechas mínima y máxima
    select trunc(min(order_date), 'MM') as min_month,
        trunc(max(order_date), 'MM') as max_month
    from orders
),
date_range as (
    -- Rellenamos la lista de meses
    select add_months(min_month, level - 1) as month
    from date_bounds
    connect by level <= months_between(max_month, min_month) + 1
),
report_sales as (
    select trunc(order_date, 'Month') as month, state_province,
       nvl(sum(quantity * unit_price), 0) as total_sales
    from customers cu, orders o, order_items oi, product_information pi
    where pi.product_id = oi.product_id
        and oi.order_id = o.order_id
        and o.customer_id = cu.customer_id
        and lower(state_province) = 'wi'
    group by month, state_province
),
report_months_sales as (
    -- Todos los meses estén presentes
    select d.month, nvl(total_sales, 0) as total_sales
    from date_range d
    left join report_sales rs on d.month = rs.month
),
final_report as (
    select month, total_sales,
        round(avg(total_sales) 
            over (order by month
            rows between 2 preceding and current row), 2) as moving_avg
    from report_months_sales
    order by month
)
select month, to_char(total_sales, '$99,999,999,999,990.00') as total_sales,
    moving_avg
from final_report
where total_sales > 0;

// 11.
with date_bounds as (
    -- Obtener las fechas mínima y máxima
    select trunc(min(order_date), 'MM') as min_month,
       trunc(max(order_date), 'MM') as max_month
    from orders
),
date_range as (
    -- Rellenamos la lista de meses
    select add_months(min_month, level - 1) as month
    from date_bounds
    connect by level <= months_between(max_month, min_month) + 1
),
report_sales as (
    select trunc(order_date, 'Month') as month,
       state_province,
       nvl(sum(quantity * unit_price), 0) as total_sales
    from customers cu, orders o, order_items oi, product_information pi
    where pi.product_id = oi.product_id
        and oi.order_id = o.order_id
        and o.customer_id = cu.customer_id
        and lower(state_province) = 'wi'
    group by month, state_province
),
report_months_sales as (
    -- Todos los meses estén presentes
    select d.month, nvl(total_sales, 0) as total_sales
    from date_range d
    left join report_sales rs on d.month = rs.month
),
final_report as (
    select month, total_sales,
        round(avg(total_sales) 
            over (order by month
            rows between 2 preceding and current row), 2) as moving_avg,
        round((total_sales - lag(total_sales) over (order by month)) /
            nullif(lag(total_sales) over (order by month), 0) * 100, 2) as growth_percentage        
    from report_months_sales
    order by month
)
select month, to_char(total_sales, '$99,999,999,999,990.00') as total_sales,
    moving_avg, nvl(growth_percentage, total_sales) || '%' as growth_percentage
from final_report
where total_sales > 0;

// 12.
select category_name,
    to_char(sum(quantity * unit_price), '$999999,990.00') as total_sales,
    count(distinct oi.product_id) as diff_prod_sold
from product_information pi, order_items oi,product_categories pc
where pi.product_id = oi.product_id
    and pi.category_id = pc.category_id
group by category_name
order by diff_prod_sold desc, total_sales desc;

// 13.
with report_category as (
    select category_name,
        to_char(sum(quantity * unit_price), '$999999,990.00') as total_sales,
        count(distinct oi.product_id) as diff_prod_sold
    from product_information pi, order_items oi,product_categories pc
    where pi.product_id = oi.product_id
        and pi.category_id = pc.category_id
    group by category_name
    order by diff_prod_sold desc, total_sales desc
)
select *
from report_category
where rownum = 1;

// 14.
with report_category as (
    select *
    from (
        select category_name,
            to_char(sum(quantity * unit_price), '$999999,990.00') as total_sales,
            count(distinct oi.product_id) as diff_prod_sold
        from product_information pi, order_items oi,product_categories pc
        where pi.product_id = oi.product_id
            and pi.category_id = pc.category_id
        group by category_name
        order by diff_prod_sold desc, total_sales desc
    )
    where rownum = 1
),
report_product as (
    select product_name, sum(quantity * unit_price) as total_sales,
        ntile(4) over (order by sum(quantity * unit_price) desc) as bucket
    from product_information pi, order_items oi,product_categories pc
    where pi.product_id = oi.product_id
        and pi.category_id = pc.category_id
        and category_name = (select category_name from report_category)
    group by product_name
)
select product_name, to_char(total_sales, 'fm$999,999,990.00') as sold, bucket,
    rank() over (partition by bucket order by total_sales desc) as bucket_rank,
    rank() over (order by total_sales desc) as overall_rank
    from report_product;

// 15.
with report_category as (
    select *
    from (
        select category_name,
            to_char(sum(quantity * unit_price), '$999999,990.00') as total_sales,
            count(distinct oi.product_id) as diff_prod_sold
        from product_information pi, order_items oi,product_categories pc
        where pi.product_id = oi.product_id
            and pi.category_id = pc.category_id
        group by category_name
        order by diff_prod_sold desc, total_sales desc
    )
    where rownum = 1
),
report_product as (
    select product_name, sum(quantity * unit_price) as total_sales,
        ntile(4) over (order by sum(quantity * unit_price) desc) as bucket
    from product_information pi, order_items oi,product_categories pc
    where pi.product_id = oi.product_id
        and pi.category_id = pc.category_id
        and category_name = (select category_name from report_category)
    group by product_name
),
report_bucket as (
    select product_name, to_char(total_sales, 'fm$999,999,990.00') as sold, bucket, total_sales,
        rank() over (partition by bucket order by total_sales desc) as bucket_rank,
        rank() over (order by total_sales desc) as overall_rank,
        nvl(lag(total_sales, 1) over (partition by bucket order by total_sales desc) - total_sales, 0) as diff
    from report_product
),
report_diff as (
    select product_name, sold, bucket, bucket_rank, overall_rank, total_sales,
        round(avg(diff) over (partition by bucket order by total_sales desc), 2) as avg_diff
    from report_bucket
)
select product_name, sold, bucket, bucket_rank, overall_rank,
    last_value(avg_diff) over (partition by bucket order by total_sales desc
        rows between unbounded preceding and unbounded following) as avg_diff
from report_diff
order by total_sales desc;

// 16.
with report_category as (
    select *
    from (
        select category_name,
            to_char(sum(quantity * unit_price), '$999999,990.00') as total_sales,
            count(distinct oi.product_id) as diff_prod_sold
        from product_information pi, order_items oi,product_categories pc
        where pi.product_id = oi.product_id
            and pi.category_id = pc.category_id
        group by category_name
        order by diff_prod_sold desc, total_sales desc
    )
    where rownum = 1
),
report_product as (
    select product_name, sum(quantity * unit_price) as total_sales,
        ntile(4) over (order by sum(quantity * unit_price) desc) as bucket
    from product_information pi, order_items oi,product_categories pc
    where pi.product_id = oi.product_id
        and pi.category_id = pc.category_id
        and category_name = (select category_name from report_category)
    group by product_name
),
report_bucket as (
    select product_name, to_char(total_sales, 'fm$999,999,990.00') as sold, bucket, total_sales,
        rank() over (partition by bucket order by total_sales desc) as bucket_rank,
        rank() over (order by total_sales desc) as overall_rank
    from report_product
),
report_diff as (
    select product_name, sold, bucket, bucket_rank, overall_rank, total_sales,
        case
            when bucket_rank = 1 then total_sales - lead(total_sales, 7) over (partition by bucket order by total_sales desc)
            when bucket_rank = 2 then total_sales - lead(total_sales, 5) over (partition by bucket order by total_sales desc)
            when bucket_rank = 3 then total_sales - lead(total_sales, 3) over (partition by bucket order by total_sales desc)
            when bucket_rank = 4 then total_sales - lead(total_sales, 1) over (partition by bucket order by total_sales desc)
            else 0
        end as telescopic_diff
    from report_bucket
)
select product_name, sold, bucket, bucket_rank, overall_rank, telescopic_diff
from report_diff
order by total_sales desc;