

import requests
import logging
from airflow.models import Variable
from airflow.exceptions import AirflowException


# def replace_value_in_tuple(original_tuple, old_value, new_value):
#     """
#     Finds and replaces all occurrences of a value in a tuple.

#     Args:
#         original_tuple: The original tuple.
#         old_value: The value to be replaced.
#         new_value: The new value.

#     Returns:
#         A new tuple with the replaced values.
#     """
#     new_list = list(original_tuple)  # Convert to list for mutability
#     for i, val in enumerate(new_list):
#         if val == old_value:
#             new_list[i] = new_value
#     return tuple(new_list)  # Convert back to tuple

#Example using list comprehension
def replace_value_in_tuple_comp(original_tuple, old_value, new_value):
    return tuple(new_value if val == old_value else val for val in original_tuple)

def getFundConnextToken():
    logging.info(f"getToken()")
    api_url = Variable.get("FC_API_URL") + "/api/auth"
    data = {
        "username": Variable.get("FC_API_USER"),
        "password": Variable.get("FC_API_PASSPOWRD")
    }
    try:
        response = requests.post(api_url, json=data)
        response.raise_for_status()
        response_data = response.json()
        token = response_data.get('access_token')
        return token
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting token: {e}")
        raise AirflowException(f"Failed to get token: {e}")
    
def getFundProfileCols():
    try:
        cols = ["fund_code",
            "amc_code",
            "fundname_th",
            "fundname_en",
            "fund_policy",
            "tax_type",
            "fif_flag",
            "dividend_flag",
            "registration_date",
            "fund_risk_level",
            "fx_risk_flag",
            "fatca_allow_flag",
            "buy_cut_off_time",
            "fst_lowbuy_val",
            "nxt_lowbuy_val",
            "sell_cut_off_time",
            "lowsell_val",
            "lowsell_unit",
            "lowbal_val",
            "lowbal_unit",
            "sell_settlement_day",
            "switching_settlement_day",
            "switch_out_flag",
            "switch_in_flag",
            "fund_class",
            "buy_period_flag",
            "sell_period_flag",
            "switch_in_periold_flag",
            "switch_out_periold_flag",
            "buy_pre_order_day",
            "sell_pre_order_day",
            "switch_pre_order_day",
            "auto_redeem_fund",
            "beg_ipo_date",
            "end_ipo_date",
            "plain_complex_fund",
            "derivatives_flag",
            "lag_allocation_day",
            "settlement_holiday_flag",
            "hyealth_insurrance",
            "previous_fund_code",
            "investor_alert",
            "isin",
            "lowbal_condition",
            "project_retail_type",
            "fund_compare_perfermance_description",
            "allocate_digit",
            "etf_flag",
            "trustee",
            "registrar",
            "register_id",
            "lmts_notice_period_amount",
            "lmts_notice_perios_perc_aum",
            "lmts_adls_amount",
            "lmts_adls_perc_aum",
            "lmts_liquidity_fee_amount",
            "lmts_liquidity_fee_perc_aum",
            "other_information_url",
            "currency",
            "complex_fund_presentation",
            "risk_acknowledgement_of_complex_fund",
            "redemption_type_condition",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler",
            "Filler"]
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get fund profile columns {e}")
        raise AirflowException(f"Failed to get token: {e}")

def getNavColsV2():
    try:
        cols = [
            "amc_code",
            "fund_code",
            "aum",
            "nav",
            "offer_nav",
            "bid_nav",
            "switch_out_nav",
            "switch_in_nav",
            "nav_date",
            "filler"
        ]
        
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get fund profile columns {e}")
        raise AirflowException(f"Failed to get token: {e}")
    
def getNavColsV3():
    try:
        cols = [
            "amc_code",
            "fund_code",
            "aum",
            "nav",
            "offer_nav",
            "bid_nav",
            "switch_out_nav",
            "switch_in_nav",
            "nav_date",
            "sa_code_for_unit_link",
            "total_unit",
            "total_aum_all_share_class",
            "total_unit_all_share_class",
            "filler",
            "filler",
            "filler",
            "filler",
            "filler",
            "filler",
            "filler"
        ]
        
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get fund NAV columns {e}")
        raise AirflowException(f"Failed to get NAV: {e}")

def getNavColsV3():
    try:
        cols = [
            "amc_code",
            "fund_code",
            "aum",
            "nav",
            "offer_nav",
            "bid_nav",
            "switch_out_nav",
            "switch_in_nav",
            "nav_date",
            "sa_code_for_unit_link",
            "total_unit",
            "total_aum_all_share_class",
            "total_unit_all_share_class",
            "filler",
            "filler",
            "filler",
            "filler",
            "filler",
            "filler",
            "filler"
        ]
        
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get NAV columns {e}")
        raise AirflowException(f"Failed to get NAV: {e}")
    
def getBalanceCols():
    try:
        cols = [
            'amc_code',
            'accountId',
            'unitholderId',
            'fundCode',
            'unitBalance',
            'amount',
            'availableUnitBalance',
            'availableAmount',
            'pendingUnit',
            'pendingAmount',
            'pledgeUnit',
            'averageCost',
            'nav',
            'nav_date'
        ]
        
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get fund Balance columns {e}")
        raise AirflowException(f"Failed to get Balance: {e}")
    

def getCustomerINDCols():
    try:
        cols = [
            "identificationCardType",
            "passportCountry",
            "cardNumber",
            "cardExpiryDate",
            "accompanyingDocument",
            "title",
            "titleOther",
            "enFirstName",
            "enLastName",
            "thFirstName",
            "thLastName",
            "birthDate",
            "nationality",
            "mobileNumber",
            "email",
            "phone",
        "fax",
        "maritalStatus",
        # "spouse",
        "occupationId",
        "occupationOther",
        "businessTypeId",
        "businessTypeOther",
        "monthlyIncomeLevel",
        "assetValue",
        "incomeSource",
        "incomeSourceOther",
        "companyName",
        "currentAddressSameAsFlag",
        "workPosition",
        "relatedPoliticalPerson",
        "politicalRelatedPersonPosition",
        "canAcceptFxRisk",
        "canAcceptDerivativeInvestment",
        "fatca",
        "fatcaDeclarationDate",
        "cddScore",
        "cddDate",
        "referralPerson",
        "applicationDate",
        "incomeSourceCountry",
        "acceptedBy",
        "openFundConnextFormFlag",
        "approvedDate",
        "approvedDateTime",
        "openChannel",
        "investorClass",
        "vulnerableFlag",
        "vulnerableDetail",
        "ndidFlag",
        "ndidRequestId",
        "investorType",
        # "knowledgeAssessmentForm",
        "knowledgeAssessmentResult",
        ]
        
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get fund profile columns {e}")
        raise AirflowException(f"Failed to get token: {e}")

def getAccountCols():
    try:
        cols = [
            "identificationCardType",
            "passportCountry",
            "cardNumber",
            "accountId",
            "icLicense",
            "accountOpenDate",
            "investmentObjective",
            "investmentObjectiveOther",
            "approvedDate",
            "mailingAddressSameAsFlag",
        ]
        
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get fund profile columns {e}")
        raise AirflowException(f"Failed to get token: {e}")

def getUnitholderCols():
    try:
        cols = [
            "accountId",
            "unitholderId",
            "unitholderType",
            "amcCode",
            "status",
            "currency",
        ]
        
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get fund profile columns {e}")
        raise AirflowException(f"Failed to get token: {e}")
    
def getBankAccountCols():
    try:
        cols = [
            "accountId",
            "bankCode",
            "bankBranchCode",
            "bankAccountNo",
            "default",
            "currency",
            "saReferenceLog",
            "ddrTimestampReference",
            "Purpose",
        ]
        
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get fund profile columns {e}")
        raise AirflowException(f"Failed to get token: {e}")
    
def getAddressCols():
    try:
        cols = [
            "cardNumber",
            "addrType",
            "no",
            "floor",
            "building",
            "roomNo",
            "soi",
            "road",
            "moo",
            "subdistrict",
            "district",
            "province",
            "postalCode",
            "country",
        ]
        
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get fund profile columns {e}")
        raise AirflowException(f"Failed to get token: {e}")
    
def getSuitCols():
    try:
        cols = [
            "cardNumber",
            "suitabilityEvaluationDate",
            "suitabilityRiskLevel",
            "suitNo1",
            "suitNo2",
            "suitNo3",
            "suitNo4",
            "suitNo5",
            "suitNo6",
            "suitNo7",
            "suitNo8",
            "suitNo9",
            "suitNo10",
            "suitNo11",
            "suitNo12",
        ]
        
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get fund profile columns {e}")
        raise AirflowException(f"Failed to get token: {e}")

def getCrsCols():
    try:
        cols = [
            "cardNumber",
            "tin",
            "reason",
            "reasonDesc",
            "countryOfTaxResidence",
            "placeOfBirthCountry",
            "placeOfBirthCity",
            "taxResidenceInCountriesOtherThanTheUS",    
            "declarationDate",
        ]
        
        return cols
    except requests.exceptions.RequestException as e:
        logging.error(f"get fund profile columns {e}")
        raise AirflowException(f"Failed to get token: {e}")