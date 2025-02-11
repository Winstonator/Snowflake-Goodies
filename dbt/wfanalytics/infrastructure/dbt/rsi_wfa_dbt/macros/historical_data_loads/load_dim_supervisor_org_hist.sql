{% macro load_dim_supervisor_org_hist() %}
    {% do log("Creating historical load for supervisor organization", info=True) %}

        {% set sql %}
            CREATE TABLE IF NOT EXISTS {{target.database}}.CORE.DIM_SUPERVISOR_ORG AS
            WITH 
            SUPERVISOR_ORG AS ( SELECT
                ORG_ID AS ORG_ID,
                ORG_DISPLAY_ID AS ORG_DISPLAY_ID,
                OWNER_EIN AS OWNER_EMP_ID,
                OWNER_POSITION_ID AS OWNER_POSITION_ID,
                SUPERIOR_EIN AS SUP_EMP_ID,
                SUPERIOR_MGR_POSITION_ID AS SUP_MGR_POSITION_ID,
                ORG_NAME AS ORG_NAME,
                ORG_FULL_PATH AS ORG_FULL_PATH,
                PRIMARY_LOCATION AS PRIM_LOCATION,
                SUPERIOR_ORG_NAME AS SUP_ORG_NAME,
                SUPERIOR_ORG_EFFECTIVE_DT AS SUP_ORG_EFF_DT,
                IMMEDIATE_SUPERIOR_ORGS AS IMMEDIATE_SUP_ORGS,
                IMMEDIATE_SUBORDINATE_ORGS AS IMMEDIATE_SUBORDINATE_ORGS,
                STAFFING_MODEL AS STAFFING_MODEL,
                RPT_EFFECTIVE_DT AS RPT_EFF_DT,
                ROW_NUMBER() OVER (PARTITION BY
                    ORG_ID,
                    ORG_DISPLAY_ID,
                    OWNER_EIN,
                    OWNER_POSITION_ID,
                    SUPERIOR_EIN,
                    SUPERIOR_MGR_POSITION_ID,
                    ORG_NAME,
                    ORG_FULL_PATH,
                    PRIMARY_LOCATION,
                    SUPERIOR_ORG_NAME,
                    SUPERIOR_ORG_EFFECTIVE_DT,
                    IMMEDIATE_SUPERIOR_ORGS,
                    IMMEDIATE_SUBORDINATE_ORGS,
                    STAFFING_MODEL
                    ORDER BY RPT_EFFECTIVE_DT) AS RN
            FROM {{ source('STAGING','STG_WKD_SUPERVISOR_ORG') }}
            QUALIFY RN =1 ),

            VERSION AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID ORDER BY RPT_EFF_DT DESC) AS VERSION_NUMBER FROM SUPERVISOR_ORG
            ),

            FINAL AS (
                SELECT V1.*, V1.RPT_EFF_DT AS EFF_DT, V2.RPT_EFF_DT -1  AS DISC_DT
                FROM VERSION V1 
                LEFT JOIN VERSION V2 ON V1.VERSION_NUMBER -1 =V2.VERSION_NUMBER AND V1.ORG_ID = V2.ORG_ID 
                ORDER BY V1.VERSION_NUMBER
            )

            SELECT 
                {{ dbt_utils.surrogate_key(['ORG_ID','RPT_EFF_DT']) }} AS SUP_ORG_SK,
                ORG_ID,
                ORG_DISPLAY_ID,
                OWNER_EMP_ID,
                OWNER_POSITION_ID,
                SUP_EMP_ID,
                SUP_MGR_POSITION_ID,
                ORG_NAME,
                ORG_FULL_PATH,
                PRIM_LOCATION,
                SUP_ORG_NAME,
                SUP_ORG_EFF_DT,
                IMMEDIATE_SUP_ORGS,
                IMMEDIATE_SUBORDINATE_ORGS,
                STAFFING_MODEL,
                RPT_EFF_DT,
                'WKD' AS REC_SRC,
                EFF_DT, 
                COALESCE(DISC_DT, '9999-12-31') as DISC_DT,
                CASE WHEN DISC_DT IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT,         
                TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS INS_BATCH_ID,
                TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS UPD_BATCH_ID,
                {{ dbt_utils.surrogate_key([
                    'ORG_ID',
                    'ORG_DISPLAY_ID',
                    'OWNER_EMP_ID',
                    'OWNER_POSITION_ID',
                    'SUP_EMP_ID',
                    'SUP_MGR_POSITION_ID',
                    'ORG_NAME',
                    'ORG_FULL_PATH',
                    'PRIM_LOCATION',
                    'SUP_ORG_NAME',
                    'SUP_ORG_EFF_DT',
                    'IMMEDIATE_SUP_ORGS',
                    'IMMEDIATE_SUBORDINATE_ORGS',
                    'STAFFING_MODEL']) }} AS DBT_HASH
            FROM FINAL;

        {% endset %}
        
        {{ return(sql) }}

    {% do log("historical compensation grade profile table created", info=True) %}

{% endmacro %}