

import requests
import logging
from airflow.models import Variable


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
    