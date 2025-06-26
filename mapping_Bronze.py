# Databricks notebook source
from pyspark.sql.types import *
#StringType,IntegerType,DateType,TimestampType,LongType,StringType,StructField,DecimalType,BooleanType,FloatType

# COMMAND ----------

schema_ContractRemittanceCancelSupport = StructType([
     StructField("ContractKey", LongType()),
     StructField("ContractNumber", StringType()),
     StructField("SequenceNumber", IntegerType()),
     StructField("AgreementNumber", StringType()),
     StructField("ProductId", IntegerType()),
     StructField("RemitAmountTypeKey", LongType()),
     StructField("RemitPaidFromKey", LongType()),
     StructField("RemitTypeKey", LongType()),
     StructField("PayTo", StringType()),
     StructField("CheckTo", StringType()),
     StructField("Amount", DecimalType()),
     StructField("ReferenceNumber", StringType()),
     StructField("DateMailed", TimestampType()),
     StructField("CancelReferenceDate", TimestampType()),
     StructField("DateAppliedUtc", TimestampType()),
     StructField("CMDBFlag", IntegerType()),
     StructField("CancelSupportId", LongType()),
     StructField("AgreementNo", StringType()),
     StructField("SeqNo", LongType()),
     StructField("ContractRemittanceCancelSupportKey", LongType()),
     StructField("CutOffEffectiveDateTimeUTC", TimestampType()),
     StructField("CutOffExpirationDateTimeUTC", TimestampType())
     ])

# COMMAND ----------

schema_RemitAmountType = StructType([
     StructField("RemitAmountTypeKey", LongType()),
     StructField("RemitAmountTypeCode", StringType()),
     StructField("RemitAmountTypeDescription", StringType()),
     StructField("CutOffEffectiveDateTimeUTC", TimestampType()),
     StructField("CutOffExpirationDateTimeUTC", TimestampType())
    ])

# COMMAND ----------

schema_RemitPaidFrom = StructType([
     StructField("RemitPaidFromKey", LongType()),
     StructField("RemitPaidFromCode", StringType()),
     StructField("RemitPaidFromDescription", StringType()),
     StructField("CutOffEffectiveDateTimeUTC", TimestampType()),
     StructField("CutOffExpirationDateTimeUTC", TimestampType())
    ])

# COMMAND ----------

schema_RemitType = StructType([
     StructField("RemitTypeKey", LongType()),
     StructField("RemitTypeCode", StringType()),
     StructField("RemitTypeDescription", StringType()),
     StructField("CutOffEffectiveDateTimeUTC", TimestampType()),
     StructField("CutOffExpirationDateTimeUTC", TimestampType())
    ])

# COMMAND ----------

schema_ContractCommission = StructType([
     StructField("ContractKey", LongType()),
     StructField("ContractNumber", StringType()),
     StructField("AgentKey", LongType()),
     StructField("AgentCommissionTypeKey", LongType()),
     StructField("AgentType", StringType()),
     StructField("RefundTypeKey", LongType()),
     StructField("Payee", StringType()),
     StructField("Refunder", StringType()),
     StructField("Fee", DecimalType()),
     StructField("CancelFee", IntegerType()),
     StructField("CMDBFlag", StringType()),
     StructField("ID", LongType()),
     StructField("CutOffEffectiveDateTimeUTC", TimestampType()),
     StructField("CutOffExpirationDateTimeUTC", TimestampType())
    ])

# COMMAND ----------


schema_MonthlySummaryStaging = StructType([
     StructField("ContractKey", LongType()),
     StructField("ContractNumber", StringType()),
     StructField("SequenceNumber", IntegerType()),
     StructField("AgreementKey", LongType()),
     StructField("AgreementNumber", StringType()),
     StructField("ProductId", IntegerType()),
     StructField("ObligorCode", StringType()),
     StructField("CarrierCode", StringType()),
     StructField("SellerCode", StringType()),
     StructField("SellerName", StringType()),
     StructField("FulfillmentCompanyDescription", StringType()),
     StructField("ContractStatusDescription", StringType()),
     StructField("OriginalState", StringType()),
     StructField("BatchNumber", StringType()),
     StructField("ALTMajorGroupCode", StringType()),
     StructField("StartOdometer", LongType()),
     StructField("TermMonths", IntegerType()),
     StructField("ContractSaleDate", TimestampType()),
     StructField("InitialSaleDate", TimestampType()),
     StructField("ExpirationDate", TimestampType()),
     StructField("GrossPremium", DecimalType()),
     StructField("IsMonthToMonth", IntegerType()),
     StructField("CancelPercent", DecimalType()),
     StructField("ContractEffectiveDate", TimestampType()),
     StructField("ContractType", StringType()),
     StructField("MileageBandTableCode", StringType()),
     StructField("MileageBandMin", IntegerType()),
     StructField("MileageBandMax", IntegerType()),
     StructField("ZAlphaAnalysis", StringType()),
     StructField("ClipType", StringType()),
     StructField("ReinsuranceFeePercent", DecimalType()),
     StructField("CurrentTermMonth", LongType()),
     StructField("PartialDays", LongType()),
     StructField("TotalDays", LongType()),
     StructField("CalDays", LongType()),
     StructField("CurrentEarningRate", DecimalType()),
     StructField("NextEarningRate", DecimalType()),
     StructField("AnalyticalEarningRate", DecimalType()),
     StructField("ProrataEarningRate", DecimalType()),
     StructField("OriginalActuarialEarningRate", DecimalType()),
     StructField("ActuarialEarningRate", DecimalType()),
     StructField("GrossReserve", DecimalType()),
     StructField("NetReserve", DecimalType()),
     StructField("GrossMBIReserve", DecimalType()),
     StructField("NetMBIReserve", DecimalType()),
     StructField("GrossFeeTotal", DecimalType()),
     StructField("NetFeeTotal", DecimalType()),
     StructField("GrossMBIFeeTotal", DecimalType()),
     StructField("NetMBIFeeTotal", DecimalType()),
     StructField("GrossSalesTaxFee", DecimalType()),
     StructField("NetSalesTaxFee", DecimalType()),
     StructField("GrossAgentFee", DecimalType()),
     StructField("NetAgentFee", DecimalType()),
     StructField("RateTotal", DecimalType()),
     StructField("GrossReserveInclMBI", DecimalType()),
     StructField("NetReserveInclMBI", DecimalType()),
     
     StructField("GrossFeeTotalInclMBI", DecimalType()),
     StructField("CalculatedSellerCost", DecimalType()),
     StructField("PaidAmount", DecimalType()),
     StructField("PendingAmount", DecimalType()),
     StructField("CaseReserveAllNoAuth", DecimalType()),
     StructField("CaseReservesMainAuth", DecimalType()),
     StructField("CaseReservesOtherAuth", DecimalType()),
     StructField("CaseReservesTotal", DecimalType()),
     StructField("AnalyticalEarnedReserve", DecimalType()),
     StructField("ProrataEarnedReserve", DecimalType()),
     StructField("ActuarialEarnedReserve", DecimalType()),
     StructField("IBNRAllNoSetup", DecimalType()),
     StructField("SSOAdjReserves", DecimalType()),
     StructField("SSAdjReserves", DecimalType()),
     StructField("SSOAdjustedPaidClaim", DecimalType()),
     StructField("SSAdjustedPaidClaim", DecimalType()),
     StructField("SSOEstimatedLoss", DecimalType()),
     StructField("SSEstimatedLoss", DecimalType()),
     StructField("ContractCutOffDateTimeUTC", TimestampType()),
     StructField("InforceDate", DateType()),
     StructField("AverageDaysOfMonth", DecimalType()),
     StructField("SetEffectiveDate", TimestampType()),
     StructField("CutOffPrimaryKey", LongType()),
     StructField("CutOffEffectiveDateTimeUTC", TimestampType()),
     StructField("CutOffExpirationDateTimeUTC", TimestampType())
     ])

# COMMAND ----------

# Storing schemas in a dictionary
schemas = {
    "schema_ContractRemittanceCancelSupport":schema_ContractRemittanceCancelSupport,
    "schema_RemitAmountType":schema_RemitAmountType,
    "schema_RemitPaidFrom":schema_RemitPaidFrom,
    "schema_RemitType":schema_RemitType,
    "schema_ContractCommission":schema_ContractCommission,
    "schema_MonthlySummaryStaging":schema_MonthlySummaryStaging
}
