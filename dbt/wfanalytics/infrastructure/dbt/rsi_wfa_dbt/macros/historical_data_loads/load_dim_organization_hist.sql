{% macro load_dim_organization_hist() %}
    {% do log("Creating historical load for organization", info=True) %}

        {% set create_table_stmt %}
            CREATE OR REPLACE TABLE {{target.database}}.CORE.DIM_ORGANIZATION AS
            SELECT
            {{ dbt_utils.surrogate_key(['DIVISION_CD','LOCAL_USE_CD','DEPARTMENT_CD','EFF_DT'])}} as ORG_SK,
            {{ dbt_utils.surrogate_key(['DIVISION_CD','LOCAL_USE_CD','DEPARTMENT_CD'])}} as ORG_PK,
            {{ dbt_utils.surrogate_key([
                'COST_CENTER_CD'
                ,'COST_CENTER_DESC'
                ,'COST_CENTER_ID'
                ,'LEGAL_ENTITY_CD'
                ,'LEGAL_ENTITY_DESC'
                ,'REGION_CD'
                ,'REGION_DESC'
                ,'AREA_CD'
                ,'AREA_DESC'
                ,'BUSINESS_UNIT_CD'
                ,'BUSINESS_UNIT_DESC'
                ,'DIVISION_CD'
                ,'DIVISION_DESC'
                ,'LOB_CATEGORY'
                ,'LOB_CD'
                ,'LOB_DESC'
                ,'DEPARTMENT_CD'
                ,'DEPARTMENT_DESC'
                ,'LOCAL_USE_CD'
                ,'LOCAL_USE_DESC'
                ,'CURR_REGION_CD'
                ,'CURR_REGION_DESC'
                ,'CURR_AREA_CD'
                ,'CURR_AREA_DESC'
                ,'CURR_BUSINESS_UNIT_CD'
                ,'CURR_BUSINESS_UNIT_DESC'
                ,'CURR_DIVISION_CD'
                ,'CURR_DIVISION_DESC'
                ,'CURR_DEPARTMENT_DESC'
                ,'CURR_DEPARTMENT_CD'
                ,'CURR_LEGAL_ENTITY_CD'
                ,'CURR_LEGAL_ENTITY_DESC'
                ,'CURR_LOB_CD'
                ,'CURR_LOB_DESC'
                ,'CURR_COST_CENTER_CD'
                ,'CURR_COST_CENTER_DESC'
                ,'CURR_COST_CENTER_ID'
                ,'CURR_LOB_CATEGORY'
                ,'CURR_LOCAL_USE_CD'
                ,'CURR_LOCAL_USE_DESC'
                ]) }} AS DBT_HASH,
            COST_CENTER_CD,
            COST_CENTER_DESC,
            COST_CENTER_ID,
            LEGAL_ENTITY_CD,
            LEGAL_ENTITY_DESC,
            REGION_CD,
            REGION_DESC,
            AREA_CD,
            AREA_DESC,
            BUSINESS_UNIT_CD,
            BUSINESS_UNIT_DESC,
            DIVISION_CD,
            DIVISION_DESC,
            LOB_CATEGORY,
            LOB_CD,
            LOB_DESC,
            DEPARTMENT_CD,
            DEPARTMENT_DESC,
            LOCAL_USE_CD,
            LOCAL_USE_DESC,
            CURR_REGION_CD,
            CURR_REGION_DESC,
            CURR_AREA_CD,
            CURR_AREA_DESC,
            CURR_BUSINESS_UNIT_CD,
            CURR_BUSINESS_UNIT_DESC,
            CURR_DIVISION_CD,
            CURR_DIVISION_DESC,
            CURR_DEPARTMENT_DESC,
            CURR_DEPARTMENT_CD,
            CURR_LEGAL_ENTITY_CD,
            CURR_LEGAL_ENTITY_DESC,
            CURR_LOB_CD,
            CURR_LOB_DESC,
            CURR_COST_CENTER_CD,
            CURR_COST_CENTER_DESC,
            CURR_COST_CENTER_ID,
            CURR_LOB_CATEGORY,
            CURR_LOCAL_USE_CD,
            CURR_LOCAL_USE_DESC,
            REC_SRC,
            CREATED_DTM,
            EFF_DT as RPT_EFF_DT,
            DISC_DT,
            IS_CURRENT,
            INS_BATCH_ID,
            UPD_BATCH_ID
            FROM
            (
            WITH
            FEIN AS (    
                SELECT FEIN
                        , COMPANY_NAME
                        , TRIM(SUBSTRING(COST_CENTER,1,4)) as FEIN_DIV_NBR
                from {{ source('STAGING','STG_WKD_COMPANY') }}
                where upper(cc_code) not like 'CC_UPDATE%'
                qualify row_number() over(partition by TRIM(SUBSTRING(COST_CENTER,1,4)) order by rpt_run_dtm desc) = 1
            ),
            STG_WKD_COMPANY AS (
                select TRIM(SUBSTRING(COST_CENTER,1,4)) as COM_DIV_NBR
                , CASE WHEN TRIM(SUBSTRING(COST_CENTER,8,4)) < '1000' THEN '0000'
                    WHEN TRIM(SUBSTRING(COST_CENTER,8,4)) = 'ARCH' THEN '0000'
                    ELSE TRIM(SUBSTRING(COST_CENTER,8,4)) END as COM_LOCAL_USE
                , TRIM(SUBSTRING(COST_CENTER,15,4)) as COM_DEPARTMENT_NBR
                , CASE WHEN TRIM(SUBSTRING(COST_CENTER,8,4)) < '1000' THEN 
                    CONCAT (SUBSTR(CC_CODE,1,7), '0000', SUBSTR(CC_CODE,12,1000))
                    WHEN TRIM(SUBSTRING(COST_CENTER,8,4)) = 'ARCH' THEN 
                    CONCAT (SUBSTR(CC_CODE,1,7), '0000', SUBSTR(CC_CODE,12,1000))
                    ELSE CC_CODE END as CC_CODE_CORRECTED -- CC_CODE
                , CASE WHEN TRIM(SUBSTRING(COST_CENTER,8,4)) < '1000' THEN
                    CONCAT (SUBSTR(COST_CENTER,1,7), '0000', SUBSTR(COST_CENTER,12,1000))
                    WHEN TRIM(SUBSTRING(COST_CENTER,8,4)) = 'ARCH' THEN
                    CONCAT (SUBSTR(COST_CENTER,1,7), '0000', SUBSTR(COST_CENTER,12,1000))
                    ELSE COST_CENTER END as COM_COST_CENTER_DESC_CORRECTED -- COST_CENTER as COM_COST_CENTER_DESC,
                , CASE WHEN TRIM(SUBSTRING(COST_CENTER,8,4)) < '1000' THEN
                    CONCAT (SUBSTR(COST_CENTER,1,7), '0000 -', SUBSTR(COST_CENTER,14,4))
                    WHEN TRIM(SUBSTRING(COST_CENTER,8,4)) = 'ARCH' THEN 
                    CONCAT (SUBSTR(COST_CENTER,1,7), '0000 -', SUBSTR(COST_CENTER,14,4))
                    ELSE SUBSTRING(COST_CENTER,1,17)  END as COM_COST_CENTER_ID_CORRECTED
                --  SUBSTRING(COST_CENTER,1,17) as COM_COST_CENTER_ID, 
                , 'WKD' AS REC_SOURCE
                from STAGING.STG_WKD_COMPANY
                where upper(cc_code) not like 'CC_UPDATE%'
                qualify row_number() over(partition by CC_CODE_CORRECTED order by rpt_run_dtm desc) = 1
            ),
            DIM_CORP_HIER AS (
                select -- NVL(DCH.local_use,'0000') as LCL_USE
                {{ dbt_utils.surrogate_key(['DIV_NBR','SUB_LOB'])}} as DCH_HASH_KEY
                , REGION_NBR as DCH_REGION_NBR
                , REGION_NM as DCH_REGION_NM
                , AREA_NBR as DCH_AREA_NBR
                , AREA_NM as DCH_AREA_NM
                , BU_NBR as DCH_BU_NBR
                , BU_DESC as DCH_BU_DESC
                , DIV_NBR as DCH_DIV_NBR
                , DIV_NM as DCH_DIV_NM
                , LOB_CATEGORY as DCH_LOB_CATEGORY
                , LOB as DCH_LOB
                , LOB_DESC as DCH_LOB_DESC
                , SUB_LOB as DCH_DEPARTMENT_NBR
                , SUB_LOB_DESC as DCH_DEPARTMENT_DESC
                , LOCAL_USE as DCH_LOCAL_USE
                , LOCAL_DESC as DCH_LOCAL_DESC
                , CUR_REGION_NBR as DCH_CUR_REGION_NBR
                , CUR_REGION_NM as DCH_CUR_REGION_NM
                , CUR_AREA_NBR as DCH_CUR_AREA_NBR
                , CUR_AREA_NM as DCH_CUR_AREA_NM
                , CUR_BU_NBR as DCH_CUR_BU_NBR
                , CUR_BU_DESC as DCH_CUR_BU_DESC
                , CUR_DIV_NBR as DCH_CUR_DIV_NBR
                , CUR_DIV_NM as DCH_CUR_DIV_NM
                , CUR_SUB_LOB_DESC as DCH_CUR_SUB_LOB_DESC
                , CUR_SUB_LOB as DCH_CUR_SUB_LOB
                , CUR_LOB as DCH_CUR_LOB
                , CUR_LOB_DESC as DCH_CUR_LOB_DESC
                , CUR_LOB_CATEGORY as DCH_CUR_LOB_CATEGORY
                , SOURCE_SYSTEM as DCH_SOURCE_SYSTEM
                , (CASE WHEN DISC_DT IS NULL THEN '1950-01-01' ELSE EFF_DT END)::DATE as DCH_EFF_DT
                , (COALESCE(DECODE(DISC_DT::DATE,'1816-03-29',NULL,DISC_DT), '9999-12-31'))::DATE as DCH_DISC_DT
                , IS_CURRENT as DCH_IS_CURRENT
                from {{ source('EDW_CORE','DIM_CORP_HIER') }} DCH
                where is_current = true and source_system = 'OFC'
                qualify ROW_NUMBER()
                    OVER(PARTITION BY REGION_NBR, AREA_NBR, BU_NBR, DIV_NBR, LOCAL_USE, SUB_LOB
                        ORDER BY DCH_EFF_DT) = 1
            ),
            STG_DIM_ORGANIZATION AS (
                select
                {{ dbt_utils.surrogate_key(['CURDIVISIONCODE','CURDEPARTMENTID'])}} AS SDO_HASH_KEY
                , {{ dbt_utils.surrogate_key(['BUSINESSUNITCODE','DIVISIONCODE','DEPARTMENTID','EFFECTIVEDATE','DEPARTMENTCODE'])}} AS RM_HASH_KEY
                , CASE WHEN EFFECTIVEDATE = '20211131' then '20211130' else effectivedate end as EFF_DT
                , *
                from {{ source('STAGING','STG_DIM_ORGANIZATION')}} sdo
                where rm_hash_key != '26d80d15d7bce58bbfbe9ba74e44c2a6'
            ),
            ORGANIZATION AS (
                SELECT  
                    null as ORG_SK, -- defined above
                    null AS DBT_HASH, -- defined above
                    COM.CC_CODE_CORRECTED as COST_CENTER_CD,
                    COM.COM_COST_CENTER_DESC_CORRECTED as COST_CENTER_DESC,
                    COM.COM_COST_CENTER_ID_CORRECTED AS COST_CENTER_ID,
                    coalesce(nullif(trim(FEIN.FEIN),''), SDO.LegalEntityCode) as LEGAL_ENTITY_CD,
                    coalesce(nullif(trim(FEIN.COMPANY_NAME),''), SDO.LegalEntityDesc) as LEGAL_ENTITY_DESC,
                    coalesce(nullif(trim(SDO.RegionCode),''), DCH.DCH_REGION_NBR) as REGION_CD,
                    coalesce(nullif(trim(SDO.RegionDesc),''), DCH.DCH_REGION_NM ) as REGION_DESC,
                    coalesce(nullif(trim(SDO.AreaCode),''), DCH.DCH_AREA_NBR) as AREA_CD,
                    coalesce(nullif(trim(SDO.AreaDesc),''), DCH.DCH_AREA_NM) as AREA_DESC,
                    coalesce(nullif(trim(SDO.BusinessUnitCode),''), DCH.DCH_BU_NBR) as BUSINESS_UNIT_CD,
                    coalesce(nullif(trim(SDO.BusinessUnitDesc),''), DCH.DCH_BU_DESC) as BUSINESS_UNIT_DESC,
                    coalesce(nullif(trim(SDO.DivisionCode),''), DCH.DCH_DIV_NBR) as DIVISION_CD,
                    coalesce(nullif(trim(SDO.DivisionDesc),''), DCH.DCH_DIV_NM) as DIVISION_DESC,
                    DCH.DCH_LOB_CATEGORY as LOB_CATEGORY,
                    coalesce(nullif(trim(SDO.LineOfBusinessCode),''), DCH.DCH_LOB) as LOB_CD,
                    coalesce(nullif(trim(SDO.LineOfBusinessDesc),''), DCH.DCH_LOB_DESC) as LOB_DESC,
                    coalesce(nullif(trim(SDO.DepartmentID),''), DCH.DCH_DEPARTMENT_NBR) as DEPARTMENT_CD,
                    coalesce(nullif(trim(DCH.DCH_DEPARTMENT_DESC),''), SDO.DepartmentDesc) as DEPARTMENT_DESC,
                    coalesce(nullif(trim(DCH.DCH_LOCAL_USE),''), '0000') as LOCAL_USE_CD,
                    DCH.DCH_LOCAL_DESC as LOCAL_USE_DESC,
                    coalesce(nullif(trim(SDO.CurRegionCode),''), DCH.DCH_CUR_REGION_NBR) as CURR_REGION_CD,
                    coalesce(nullif(trim(SDO.CurRegionDesc),''), DCH.DCH_CUR_REGION_NM) as CURR_REGION_DESC,
                    coalesce(nullif(trim(SDO.CurAreaCode),''), DCH.DCH_CUR_AREA_NBR) as CURR_AREA_CD,
                    coalesce(nullif(trim(SDO.CurAreaDesc),''), DCH.DCH_CUR_AREA_NM) as CURR_AREA_DESC,
                    coalesce(nullif(trim(SDO.CurBusinessUnitCode),''), DCH.DCH_CUR_BU_NBR) as CURR_BUSINESS_UNIT_CD,
                    coalesce(nullif(trim(SDO.CurBusinessUnitDesc),''), DCH.DCH_CUR_BU_DESC) as CURR_BUSINESS_UNIT_DESC,
                    coalesce(nullif(trim(SDO.CurDivisionCode),''), DCH.DCH_CUR_DIV_NBR) as CURR_DIVISION_CD,
                    coalesce(nullif(trim(SDO.CurDivisionDesc),''), DCH.DCH_CUR_DIV_NM) as CURR_DIVISION_DESC,
                    coalesce(nullif(trim(CASE WHEN REGEXP_COUNT(left(sdo.CurDepartmentDesc,1),'[0-9]') = 1
                                            THEN SUBSTR(sdo.CURDEPARTMENTDESC,5,200) ELSE SDO.CurDepartmentDesc
                                        END
                                    ),''), DCH.DCH_CUR_SUB_LOB_DESC) as CURR_DEPARTMENT_DESC,
                    coalesce(nullif(trim(SDO.CurDepartmentID),''), DCH.DCH_CUR_SUB_LOB) as CURR_DEPARTMENT_CD,
                    coalesce(nullif(trim(FEIN.FEIN),''), SDO.CurLegalEntityCode) as CURR_LEGAL_ENTITY_CD,
                    coalesce(nullif(trim(FEIN.COMPANY_NAME),''), SDO.CurLegalEntityDesc) as CURR_LEGAL_ENTITY_DESC,
                    coalesce(nullif(trim(SDO.CurLineOfBusinessCode),''), DCH.DCH_CUR_LOB) as CURR_LOB_CD,
                    coalesce(nullif(trim(SDO.CurLineOfBusinessDesc),''), DCH.DCH_CUR_LOB_DESC) as CURR_LOB_DESC,
                    COM.CC_CODE_CORRECTED as CURR_COST_CENTER_CD,
                    COM.COM_COST_CENTER_DESC_CORRECTED as CURR_COST_CENTER_DESC,
                    COM.COM_COST_CENTER_ID_CORRECTED as CURR_COST_CENTER_ID,
                    DCH.DCH_CUR_LOB_CATEGORY as CURR_LOB_CATEGORY,
                    DCH.DCH_LOCAL_USE as CURR_LOCAL_USE_CD,
                    DCH.DCH_LOCAL_DESC as CURR_LOCAL_USE_DESC,
                    SDO.OrgSource as REC_SRC,
                    TO_TIMESTAMP(SDO.CreateDate,'YYYYMMDD') as CREATED_DTM,
                    case when SDO.DivisionCode = '2034' and SDO.BusinessUnitCode = 'BU503'
                            and TO_DATE(SDO.EFF_DT,'YYYYMMDD') = '2022-04-01'
                        then TO_DATE('1950-01-01') --WellKept acquisition
                    else TO_DATE(SDO.EFF_DT, 'YYYYMMDD') end as EFF_DT,
                    case when SDO.DivisionCode = '2034' and SDO.BusinessUnitCode = 'BU503' 
                            and SDO.DepartmentID = '840' and TO_DATE(SDO.DiscontinueDate,'YYYYMMDD') = '2017-04-17'
                        then TO_DATE('2022-03-15') --WellKept acquisition
                    else TO_DATE(SDO.DiscontinueDate,'YYYYMMDD') end as DISC_DT,
                    SDO.ActiveFlag::BOOLEAN as IS_CURRENT,
                    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS INS_BATCH_ID,
                    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS UPD_BATCH_ID
                FROM STG_DIM_ORGANIZATION SDO
                LEFT JOIN DIM_CORP_HIER DCH ON DCH.DCH_HASH_KEY = SDO.SDO_HASH_KEY
                LEFT JOIN STG_WKD_COMPANY COM ON COM.COM_DIV_NBR = SDO.DIVISIONCODE --get history
                    AND COM.COM_LOCAL_USE = NVL(DCH.DCH_LOCAL_USE,'0000')
                    AND COM.COM_DEPARTMENT_NBR = SDO.DEPARTMENTID
                LEFT JOIN FEIN on FEIN.FEIN_DIV_NBR = SDO.DIVISIONCODE
            )
            SELECT
                NULL AS ORG_SK, -- defined above
                NULL AS DBT_HASH, -- defined above
                cast(COST_CENTER_CD as varchar) as COST_CENTER_CD,
                cast(COST_CENTER_DESC as varchar) as COST_CENTER_DESC,
                cast(COST_CENTER_ID as varchar) as COST_CENTER_ID,
                LEGAL_ENTITY_CD,
                LEGAL_ENTITY_DESC,
                REGION_CD,
                REGION_DESC,
                AREA_CD,
                AREA_DESC,
                BUSINESS_UNIT_CD,
                BUSINESS_UNIT_DESC,
                CAST(CAST(DIVISION_CD as int) as VARCHAR) as DIVISION_CD,
                DIVISION_DESC,
                cast(LOB_CATEGORY as varchar) as LOB_CATEGORY,
                LOB_CD,
                LOB_DESC,
                CAST(CAST(DEPARTMENT_CD as int) as VARCHAR) as DEPARTMENT_CD,
                DEPARTMENT_DESC,
                cast(LOCAL_USE_CD as varchar) as LOCAL_USE_CD,
                cast(LOCAL_USE_DESC as varchar) as LOCAL_USE_DESC,
                CURR_REGION_CD,
                CURR_REGION_DESC,
                CURR_AREA_CD,
                CURR_AREA_DESC,
                CURR_BUSINESS_UNIT_CD,
                CURR_BUSINESS_UNIT_DESC,
                CAST(CAST(CURR_DIVISION_CD as int) as VARCHAR) as CURR_DIVISION_CD,
                CURR_DIVISION_DESC,
                CURR_DEPARTMENT_DESC,
                CAST(CAST(CURR_DEPARTMENT_CD as int) as VARCHAR) as CURR_DEPARTMENT_CD,
                CURR_LEGAL_ENTITY_CD,
                CURR_LEGAL_ENTITY_DESC,
                CURR_LOB_CD,
                CURR_LOB_DESC,
                cast(CURR_COST_CENTER_CD as varchar) as CURR_COST_CENTER_CD,
                cast(CURR_COST_CENTER_DESC as varchar) as CURR_COST_CENTER_DESC,
                cast(CURR_COST_CENTER_ID as varchar) as CURR_COST_CENTER_ID,
                cast(CURR_LOB_CATEGORY as varchar) as CURR_LOB_CATEGORY,
                cast(CURR_LOCAL_USE_CD as varchar) as CURR_LOCAL_USE_CD,
                cast(CURR_LOCAL_USE_DESC as varchar) as CURR_LOCAL_USE_DESC,
                REC_SRC,
                CREATED_DTM,
                EFF_DT,
                DISC_DT,
                IS_CURRENT,
                INS_BATCH_ID,
                UPD_BATCH_ID
            FROM ORGANIZATION
            QUALIFY ROW_NUMBER() OVER (PARTITION BY
                COST_CENTER_CD
                ,COST_CENTER_DESC
                ,COST_CENTER_ID
                ,LOCAL_USE_CD
                ,LOCAL_USE_DESC
                ,REGION_CD
                ,REGION_DESC
                ,AREA_CD
                ,AREA_DESC
                ,BUSINESS_UNIT_CD
                ,BUSINESS_UNIT_DESC
                ,DIVISION_CD
                ,DIVISION_DESC
                ,DEPARTMENT_DESC
                ,DEPARTMENT_CD
                ,LEGAL_ENTITY_CD
                ,LEGAL_ENTITY_DESC
                ,LOB_CD
                ,LOB_DESC
                ,LOB_CATEGORY
                ,CURR_REGION_CD
                ,CURR_REGION_DESC
                ,CURR_AREA_CD
                ,CURR_AREA_DESC
                ,CURR_BUSINESS_UNIT_CD
                ,CURR_BUSINESS_UNIT_DESC
                ,CURR_DIVISION_CD
                ,CURR_DIVISION_DESC
                ,CURR_DEPARTMENT_DESC
                ,CURR_DEPARTMENT_CD
                ,CURR_LEGAL_ENTITY_CD
                ,CURR_LEGAL_ENTITY_DESC
                ,CURR_LOB_CD
                ,CURR_LOB_DESC
                ,CURR_COST_CENTER_CD
                ,CURR_COST_CENTER_DESC
                ,CURR_COST_CENTER_ID
                ,CURR_LOB_CATEGORY
                ,CURR_LOCAL_USE_CD
                ,CURR_LOCAL_USE_DESC ORDER BY EFF_DT DESC
                ) = 1
            )

        {% endset %}
    {% do log("Sending historical organization create table statement to model.", info=True) %}
    {{ return(create_table_stmt) }}

{% endmacro %}

