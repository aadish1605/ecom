Here's a structured document for **Derivatives Reconciliation** that outlines the required steps and queries to compare data from staging tables (STG) to the sanitized table (SAN). This document breaks down the requirements, exclusion criteria, data transformation logic, and provides pseudo-queries for each of the three staging tables: DTCC Intraday, CLS, and OCC.

---

# **Derivatives Reconciliation Document**

## **Objective**
This document details the reconciliation process between the staging tables (`STG_DERIV_DTCC_INTRADAY`, `STG_DERIV_CLS`, and `STG_DERIV_OCC`) and the sanitized data table (SAN). The goal is to ensure data accuracy by comparing counts and amounts, applying exclusion criteria, and performing currency conversion where necessary.

## **Reconciliation Steps Overview**
1. **Extract Data**: Query each staging table to get the count and amount of records.
2. **Apply Exclusion Logic**: Remove records that meet specific exclusion criteria.
3. **Transform Data**: Apply necessary transformations based on table-specific requirements.
4. **Aggregate and Summarize**: Calculate total counts and amounts after transformation.
5. **Compare with Sanitized Table**: Reconcile the aggregated values with the sanitized data.

## **Staging Tables**

### 1. **DTCC Intraday**

**Table**: `STG_DERIV_DTCC_INTRADAY`

#### **Data Extraction Criteria**
- Only process records with:
  - `baltype` = "DTC LEGAL ENTITY TOTALS" or "NSCC LEGAL ENTITY TOTALS".
  - `participantID` = 0250 or 0141.
    - *Note*: `participantID` is derived by referencing `file_nm` in `STG_RECEIVED_FILES` table using `rf_id` from `STG_DERIV_DTCC_INTRADAY`.

#### **Amount Transformation Logic**
- Each record has associated `credit` and `debit` amounts.
- If `dtccintradayinput.getDTOCTIMESTAMP()` is after 18:34 hrs:
  - `credit` amount is applied to `NR-C`.
  - `-1 * credit` amount is applied to `CP-D`.
  - `debit` amount is applied to `CP-C`.
  - `-1 * debit` amount is applied to `NR-D`.

#### **Exclusion Criteria**
- Exclude records where:
  - `participantID` = 2027 or any value other than 0250 or 0141.
  - `baltype` is not "DTC LEGAL ENTITY TOTALS" or "NSCC LEGAL ENTITY TOTALS".
  - `dtccintradayinput.getDTOCTIMESTAMP()` is before 18:34 hrs.

#### **Pseudo Query**

```sql
-- Retrieve relevant DTCC records with applied transformation and exclusion logic
SELECT 
    CASE 
        WHEN credit IS NOT NULL AND dtccintradayinput.getDTOCTIMESTAMP() > '18:34:00' THEN credit
        ELSE 0
    END AS NR_C_Credit,
    
    CASE 
        WHEN credit IS NOT NULL AND dtccintradayinput.getDTOCTIMESTAMP() > '18:34:00' THEN -1 * credit
        ELSE 0
    END AS CP_D_Debit,
    
    CASE 
        WHEN debit IS NOT NULL AND dtccintradayinput.getDTOCTIMESTAMP() > '18:34:00' THEN debit
        ELSE 0
    END AS CP_C_Credit,
    
    CASE 
        WHEN debit IS NOT NULL AND dtccintradayinput.getDTOCTIMESTAMP() > '18:34:00' THEN -1 * debit
        ELSE 0
    END AS NR_D_Debit

FROM STG_DERIV_DTCC_INTRADAY dtcc
JOIN STG_RECEIVED_FILES rf ON dtcc.rf_id = rf.rf_id
WHERE 
    dtcc.baltype IN ('DTC LEGAL ENTITY TOTALS', 'NSCC LEGAL ENTITY TOTALS') 
    AND rf.participantID IN ('0250', '0141')
    AND (dtccintradayinput.getDTOCTIMESTAMP() > '18:34:00');
```

---

### 2. **CLS**

**Table**: `STG_DERIV_CLS`

#### **Amount Transformation Logic**
- If `Payin` is `0`, then `amt` = `Payout`.
- If `Payin` is not `0`, then `amt` = `Payin`.
- The `amt` must be converted to USD using an exchange rate.

#### **Currency Conversion**
- Real-time conversion rates are stored in `STG_FX_CRNCY_RT`.
- Conversion criteria:
  - `src_amcy` = 'CAD'
  - `tgt_arncy` = 'USD'
  - `bus_dt` = '22-May-24' (replace with the appropriate business date)
  - `usr_conv_type` = 'NEW_YORK'

#### **Pseudo Query**

```sql
-- Retrieve and convert CLS records to USD
SELECT 
    CASE 
        WHEN Payin = 0 THEN Payout
        ELSE Payin
    END * fx.rate AS amt_in_usd
FROM STG_DERIV_CLS cls
JOIN STG_FX_CRNCY_RT fx 
ON fx.src_amcy = 'CAD' 
   AND fx.tgt_arncy = 'USD' 
   AND fx.bus_dt = '22-May-24' 
   AND fx.usr_conv_type = 'NEW_YORK';
```

---

### 3. **OCC**

**Table**: `STG_DERIV_OCC`

#### **Data Extraction Criteria**
- Only process records where:
  - `CMO` = 'WFCSLLC'
  - `net_settle` is not `0`

#### **Exclusion Criteria**
- Exclude records where:
  - `CMO` is not 'WFCSLLC'
  - `net_settle` is `0`

#### **Pseudo Query**

```sql
-- Retrieve relevant OCC records
SELECT 
    CMO, 
    net_settle
FROM 
    STG_DERIV_OCC
WHERE 
    CMO = 'WFCSLLC' 
    AND net_settle <> 0;
```

---

## **Final Aggregation and Comparison with Sanitized Data**

After processing each table as per the above logic, aggregate the counts and amounts, and then compare these aggregated values with the sanitized data.

### Example Query for Aggregated Comparison

```sql
-- Aggregate counts and amounts from each staging table
SELECT 
    'DTCC' AS source,
    COUNT(*) AS record_count,
    SUM(NR_C_Credit + CP_D_Debit + CP_C_Credit + NR_D_Debit) AS total_amount
FROM 
    dtcc_transformed_data -- Use the transformed DTCC data from the previous query

UNION ALL

SELECT 
    'CLS' AS source,
    COUNT(*) AS record_count,
    SUM(amt_in_usd) AS total_amount
FROM 
    cls_transformed_data -- Use the transformed CLS data from the previous query

UNION ALL

SELECT 
    'OCC' AS source,
    COUNT(*) AS record_count,
    SUM(net_settle) AS total_amount
FROM 
    occ_transformed_data -- Use the transformed OCC data from the previous query
```

---

### **Conclusion**
This document outlines the data reconciliation process between staging tables and the sanitized data. By applying the specified extraction, transformation, and exclusion logic, we ensure accurate data comparison. Once aggregated counts and amounts are obtained, these values can be reconciled against the sanitized data table to ensure alignment.

Please replace placeholders and specific logic as needed for integration into your system.
