# Databricks notebook source
insert_remitamounttype = {
    "Target.RemitAmountTypeCode": "Source.RemitAmountTypeCode",
    "Target.RemitAmountTypeDescription": "Source.RemitAmountTypeDescription",
    "Target.CutOffEffectiveDateTimeUTC": "Source.CutOffEffectiveDateTimeUTC",
    "Target.CutOffExpirationDateTimeUTC": "Source.CutOffExpirationDateTimeUTC"
}

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



