SELECT
    *
FROM nurse_staffing_db.fact_staffing_daily fsd 
LEFT JOIN (
    SELECT
        provnum,
        number_of_certified_beds as num_beds,
        provider_resides_in_hospital as in_hospital_flg,
        overall_rating
    FROM nurse_staffing_db.dim_provider
) dp
ON fsd.provnum = dp.provnum