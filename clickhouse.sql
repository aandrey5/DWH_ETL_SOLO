--CREATE DATABASE dwh_for_bi
--ENGINE = MaterializedPostgreSQL('10.128.100.98:5432', 'dwh', 'postgres', '40375mXfgbob')
--
--SETTINGS materialized_postgresql_max_block_size = 65536,
--         materialized_postgresql_tables_list = 'sales_db_copy',
--        materialized_postgresql_allow_automatic_update = 1,
--
--SELECT * FROM dwh.sales_db_copy;


--
CREATE TABLE dwh.rnc (
        type_function_ru Nullable(String),
        Federal_districts_ru Nullable(String),
        subjects2_ru Nullable(String),
        city_small_ru Nullable(String),
        distrib_ru Nullable(String),
        distrib_filial_ru Nullable(String),
        distrib_subject_ru Nullable(String),
        distrib_type_ru Nullable(String),
        aptNet_ru Nullable(String),
        aptNet2_ru Nullable(String),
        aptAdress_ru Nullable(String),
        apt_inn_ru Nullable(FixedString(100)),
        apt_inlaw_ru Nullable(String),
        aptId_ru Nullable(String),
        TMS_ru Nullable(String),
        BrandS_ru Nullable(String),
        TMS_Subcategory_ru Nullable(String),
        TMS_BG_ru Nullable(String),
        TMS_recalc_koef_ru Nullable(Int64),
        TMS_name_ru Nullable(String),
        TMS_franchaize_ru Nullable(String),
        TMS_code_ru Nullable(FixedString(200)),
        TMS_AG_ru Nullable(String),
        otdel_prodvig_ru Nullable(String),
        naklad_ru Nullable(String),
        series_ru Nullable(String),
        MP_ru Nullable(String),
        RM_ru Nullable(String),
        TM_ru Nullable(String),
        contract_rozn_ru Nullable(String),
        promo_ru Nullable(String),
        apt_agency_detail_ru Nullable(String),
        apt_profile_ru Nullable(String),
        AO_Moscow_ru Nullable(String),
        municipalitet_ru Nullable(String),
        geoCoordinate_ru Nullable(String),
        Coordinate_acc_ru Nullable(String),
        KAM_ru Nullable(String),
        market_grotex_ru Nullable(String),
        distrib_ish_ru Nullable(String),
        tms_grotex_ru Nullable(String),
        prep_ish_ru Nullable(String),
        period_year_ru FixedString(10),
        period_quarter_ru Nullable(FixedString(10)),
        period_month_ru FixedString(10),
        period_month_start_ru Nullable(FixedString(20)),
        net_grotex_ru Nullable(String),
        amount_ru Nullable(Float64),
        amount_recalc_ru Nullable(Float64),
        summa_local_ru Nullable(Float64))

 ENGINE = MergeTree() 
PARTITION BY period_month_ru
ORDER BY (period_year_ru, period_month_ru) SAMPLE BY period_month_ru SETTINGS index_granularity=8192;


--drop table dwh.rnc;

clickhouse-client --format_csv_delimiter=";" --format_csv_allow_single_quotes=0  --input_format_with_names_use_header=0 --query="INSERT INTO dwh.rnc FORMAT CSVWithNames" < 01.03.2021-31.05.2021_processed_10k.csv

try invisible character

--format_csv_delimiter=$'\x0B'

and
--format_csv_allow_single_quotes=0
or
--format_csv_allow_double_quotes=0

-- control statement

select  period_month_start_ru , TMS_ru, sum(amount_recalc_ru)  from dwh.rnc 
where type_function_ru='Продажи'
GROUP by period_month_start_ru , TMS_ru
order by period_month_start_ru, TMS_ru 
;
    


