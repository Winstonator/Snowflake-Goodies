{{
    config(
        materialized='incremental',
        unique_key ='LOCATION_SK',
        post_hook='{{ update_dim_location_disappear() }}'
        )
}}

WITH RAWDATA AS (
    SELECT *
    FROM {{ source('STAGING','STG_WKD_LOCATION') }}
        {% if is_incremental() %}
        WHERE  
        RPT_EFFECTIVE_DT > ( SELECT MAX(RPT_EFF_DT) FROM {{this}}  )
        {% endif %}
),

REQDATA AS (
    SELECT * 
    FROM {{ source('STAGING','STG_WKD_LOCATION') }} 
    WHERE LOCATION_WID IN (SELECT LOCATION_WID FROM RAWDATA )
),

LOC AS (
    SELECT
    LOCATION_REF_ID AS LOCATION_ID,
    LOCATION_WID AS LOCATION_WID,
    LOCATION_NAME AS LOCATION_NAME,
    PRIMARY_ADDRESS_LINE_1 AS PRIM_ADDRESS_LINE_1,
    PRIMARY_ADDRESS_LINE_2 AS PRIM_ADDRESS_LINE_2,
    CITY AS CITY,
    STATE AS STATE,
    PRIMARY_ADDRESS_POSTAL_CODE AS PRIM_ADDRESS_POSTAL_CD,
    COUNTRY AS COUNTRY,
    LOCATION_TYPE AS LOCATION_TYPE,
    LOCATION_USAGE AS LOCATION_USAGE,
    LOCATION_EXTERNAL_NAME AS LOCATION_EXT_NAME,
    DT_OF_LAST_CHANGE AS LAST_CHANGE_DT,
    INACTIVE AS INACTIVE,
    INACTIVE_EFFECTIVE_DT AS INACTIVE_EFF_DT,
    TIME_PROFILE AS TIME_PROFILE,
    LOCALE AS LOCALE,
    TIME_ZONE AS TIME_ZONE,
    FULL_HIERARCHY_PATH AS FULL_HIERARCHY_PATH,
    CREATED_BY AS CREATED_BY,
    CREATED_ON AS CREATED_ON,
    VISIBILITY AS VISIBILITY,
    LOC_LATITUDE AS LOC_LATITUDE,
	LOC_LONGITUDE AS LOC_LONGITUDE,
	USFIPSCOUNTYNUMBER AS US_FIPS_COUNTY_NBR,
	USFIPSSTATECODE AS US_FIPS_STATE_CD,
    RPT_EFFECTIVE_DT AS RPT_EFF_DT,
    ROW_NUMBER() OVER (PARTITION BY
        LOCATION_REF_ID,
        LOCATION_WID,
        LOCATION_NAME,
        PRIMARY_ADDRESS_LINE_1,
        PRIMARY_ADDRESS_LINE_2,
        CITY,
        STATE,
        PRIMARY_ADDRESS_POSTAL_CODE,
        COUNTRY,
        LOCATION_TYPE,
        LOCATION_USAGE,
        LOCATION_EXTERNAL_NAME,
        DT_OF_LAST_CHANGE,
        INACTIVE,
        INACTIVE_EFFECTIVE_DT,
        TIME_PROFILE,
        LOCALE,
        TIME_ZONE,
        FULL_HIERARCHY_PATH,
        CREATED_BY,
        CREATED_ON,
        VISIBILITY,
        LOC_LATITUDE,
        LOC_LONGITUDE,
        USFIPSCOUNTYNUMBER,
        USFIPSSTATECODE
        ORDER BY RPT_EFFECTIVE_DT) AS RN
FROM REQDATA
QUALIFY RN =1 ),

VERSION AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY LOCATION_WID ORDER BY RPT_EFF_DT DESC) AS VERSION_NUMBER FROM LOC
),


FINAL AS (
    SELECT V1.*, V1.RPT_EFF_DT AS EFF_DT, V2.RPT_EFF_DT -1  AS DISC_DT
    FROM VERSION V1 
    LEFT JOIN VERSION V2 ON V1.VERSION_NUMBER -1 =V2.VERSION_NUMBER AND V1.LOCATION_WID = V2.LOCATION_WID 
    ORDER BY V1.VERSION_NUMBER
)

SELECT 
    {{ dbt_utils.surrogate_key(['LOCATION_WID','RPT_EFF_DT']) }} AS LOCATION_SK,
    LOCATION_ID,
    LOCATION_WID,
    LOCATION_NAME,
    PRIM_ADDRESS_LINE_1,
    PRIM_ADDRESS_LINE_2,
    CITY,
    STATE,
    PRIM_ADDRESS_POSTAL_CD,
    COUNTRY,
    LOCATION_TYPE,
    LOCATION_USAGE,
    LOCATION_EXT_NAME,
    LAST_CHANGE_DT,
    INACTIVE,
    INACTIVE_EFF_DT,
    TIME_PROFILE,
    LOCALE,
    TIME_ZONE,
    FULL_HIERARCHY_PATH,
    CREATED_BY,
    CREATED_ON,
    VISIBILITY,
    TO_DECIMAL(LOC_LATITUDE,9,6) AS LOC_LATITUDE,
    TO_DECIMAL(LOC_LONGITUDE,9,6) AS LOC_LONGITUDE,
    TO_NUMBER(US_FIPS_COUNTY_NBR) AS US_FIPS_COUNTY_NBR,
    TO_NUMBER(US_FIPS_STATE_CD) AS US_FIPS_STATE_CD,
    RPT_EFF_DT,
    'WKD' AS REC_SRC,
    EFF_DT, 
    COALESCE(DISC_DT, '9999-12-31') as DISC_DT,
    CASE WHEN DISC_DT IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT,         
    (SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS'))) AS INS_BATCH_ID,
    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS UPD_BATCH_ID,
    {{ dbt_utils.surrogate_key(['LOCATION_ID','LOCATION_NAME','PRIM_ADDRESS_LINE_1','PRIM_ADDRESS_LINE_2','CITY','STATE','PRIM_ADDRESS_POSTAL_CD','COUNTRY','LOCATION_TYPE','LOCATION_USAGE',
    'LOCATION_EXT_NAME','LAST_CHANGE_DT','INACTIVE','INACTIVE_EFF_DT','TIME_PROFILE','LOCALE','TIME_ZONE','FULL_HIERARCHY_PATH','CREATED_BY','CREATED_ON','VISIBILITY','LOC_LATITUDE','LOC_LONGITUDE','US_FIPS_COUNTY_NBR','US_FIPS_STATE_CD']) }} AS DBT_HASH
FROM FINAL
