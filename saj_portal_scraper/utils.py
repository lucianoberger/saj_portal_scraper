# /workspaces/addons/saj_portal_scraper/utils.py
import logging
from datetime import datetime, time, date
import re

from const import (
    CONF_INACTIVITY_ENABLED,
    CONF_INACTIVITY_START_TIME,
    CONF_INACTIVITY_END_TIME,
    DEFAULT_INACTIVITY_ENABLED,
    DEFAULT_INACTIVITY_START_TIME,
    DEFAULT_INACTIVITY_END_TIME,
)

_LOGGER = logging.getLogger(__name__)

# --- Helper: soma múltiplos números dentro de uma string ---
def sum_numbers_from_string(s):
    """
    Recebe strings como:
      "22865\n22603\n22200"   -> soma linhas -> 67668.0
      "3.86-0.20-0.00-0.00"   -> soma extraindo números -> 3.66
      "9173 9064 8939"        -> soma -> 27176.0
      "3.563,06"              -> trata vírgula decimal -> 3563.06
    Retorna float (0.0 quando não encontrar números válidos).
    """
    if s is None:
        return 0.0
    s = str(s).strip()
    if s == "":
        return 0.0

    # normalizar non-break-space e vírgula decimal
    s = s.replace("\u00A0", " ").replace(",", ".")

    # split em linhas (se houver) e considerar cada linha separadamente
    lines = [ln.strip() for ln in re.split(r"[\r\n]+", s) if ln.strip() != ""]

    total = 0.0
    for ln in lines:
        # extrai todos os números na linha (inteiro ou com ponto decimal)
        parts = re.findall(r"[-+]?\d+(?:\.\d+)?", ln)
        for p in parts:
            try:
                total += float(p)
            except Exception:
                # ignora partes mal formadas
                continue

    return total


def is_inactive(config: dict) -> bool:
    """Check if the current time falls within the configured inactivity period."""
    inactivity_enabled = config.get(CONF_INACTIVITY_ENABLED, DEFAULT_INACTIVITY_ENABLED)
    if not inactivity_enabled:
        return False

    start_time_str = config.get(CONF_INACTIVITY_START_TIME, DEFAULT_INACTIVITY_START_TIME)
    end_time_str = config.get(CONF_INACTIVITY_END_TIME, DEFAULT_INACTIVITY_END_TIME)

    try:
        now_local = datetime.now()
        current_time = now_local.time()
        start_time = datetime.strptime(start_time_str, "%H:%M").time()
        end_time = datetime.strptime(end_time_str, "%H:%M").time()

        _LOGGER.debug(
            "Checking inactivity: Current local time %s, Start %s, End %s",
            current_time.strftime("%H:%M:%S"),
            start_time.strftime("%H:%M"),
            end_time.strftime("%H:%M"),
        )

        # Handle overnight inactivity period (e.g., 21:00 to 05:30)
        if start_time > end_time:
            if current_time >= start_time or current_time < end_time:
                _LOGGER.debug("Current time is within overnight inactivity period.")
                return True
        # Handle daytime inactivity period (e.g., 10:00 to 14:00)
        else:
            if start_time <= current_time < end_time:
                _LOGGER.debug("Current time is within daytime inactivity period.")
                return True

    except ValueError:
        _LOGGER.warning(
            "Invalid time format '%s' or '%s' in configuration. Skipping inactivity check.",
            start_time_str,
            end_time_str,
        )
        return False

    _LOGGER.debug("Current time is outside inactivity period.")
    return False


def aggregate_plant_data(fetched_data: dict | None) -> dict:
    """Aggregate data from all devices into a single plant summary."""
    if not fetched_data:
        _LOGGER.warning("Aggregator: No fetched data provided for aggregation.")
        return {}

    plant_sum_power = 0.0
    plant_sum_energy_today = 0.0
    plant_sum_energy_month = 0.0
    plant_sum_energy_year = 0.0
    plant_sum_energy_total = 0.0
    plant_sum_panel_power = 0.0
    latest_update_time_str: str | None = None
    latest_server_time_str: str | None = None
    compare_update_time_obj: datetime | None = None
    compare_server_time_obj: datetime | None = None

    for device_sn, latest_row_data in fetched_data.items():
        if not latest_row_data or not isinstance(latest_row_data, dict):
            _LOGGER.debug("Aggregator: No data or invalid format for device %s, skipping.", device_sn)
            continue

        device_alias = latest_row_data.get("Alias", device_sn)

        try:
            for attribute, value_str in latest_row_data.items():
                is_summable_numeric = False
                try:
                    is_summable_numeric = attribute in [
                        "Power",
                        "Energy_Today",
                        "Energy_This_Month",
                        "Energy_This_Year",
                        "Energy_Total",
                    ] or attribute.endswith("_Panel_Power")  # Also sum individual panel powers

                    if is_summable_numeric and value_str is not None:
                        # Usa o helper robusto para somar strings multi-linha/multi-número
                        value_float = sum_numbers_from_string(value_str)

                        if attribute == "Power":
                            plant_sum_power += value_float
                        elif attribute == "Energy_Today":
                            plant_sum_energy_today += value_float
                        elif attribute == "Energy_This_Month":
                            plant_sum_energy_month += value_float
                        elif attribute == "Energy_This_Year":
                            plant_sum_energy_year += value_float
                        elif attribute == "Energy_Total":
                            plant_sum_energy_total += value_float
                        elif attribute.endswith("_Panel_Power"):
                            plant_sum_panel_power += value_float

                except (ValueError, TypeError):
                    if is_summable_numeric:
                        _LOGGER.warning(
                            "Aggregator: Could not convert value '%s' to float for summing attribute '%s' in device %s. Skipping value.",
                            value_str,
                            attribute,
                            device_alias,
                        )

                # Find the latest timestamp across all devices
                if attribute == "Update_time" or attribute == "Server_Time":
                    if value_str:
                        try:
                            # Assumes 'YYYY-MM-DDTHH:MM:SSZ' format from scraper
                            iso_format_string = "%Y-%m-%dT%H:%M:%SZ"
                            current_time_obj = datetime.strptime(value_str, iso_format_string)

                            if attribute == "Update_time":
                                if (
                                    compare_update_time_obj is None
                                    or current_time_obj > compare_update_time_obj
                                ):
                                    compare_update_time_obj = current_time_obj
                                    latest_update_time_str = value_str
                            elif attribute == "Server_Time":
                                if (
                                    compare_server_time_obj is None
                                    or current_time_obj > compare_server_time_obj
                                ):
                                    compare_server_time_obj = current_time_obj
                                    latest_server_time_str = value_str
                        except (ValueError, TypeError):
                            _LOGGER.warning(
                                "Aggregator: Could not parse datetime '%s' for comparison (attribute '%s', device %s). Using raw string if latest.",
                                value_str,
                                attribute,
                                device_alias,
                            )
                            # Fallback: usa a primeira string não vazia
                            if attribute == "Update_time" and latest_update_time_str is None:
                                latest_update_time_str = value_str
                            if attribute == "Server_Time" and latest_server_time_str is None:
                                latest_server_time_str = value_str

        except Exception as e:
            _LOGGER.error(
                "Aggregator: Error processing data for device %s: %s",
                device_alias,
                e,
                exc_info=True,
            )
            continue

    # Converte potência total do plant e dos painéis para kW
    aggregated_data = {
        "Power": round(plant_sum_power / 1000, 3),  # kW
        "Energy_Today": round(plant_sum_energy_today, 2),
        "Energy_This_Month": round(plant_sum_energy_month, 2),
        "Energy_This_Year": round(plant_sum_energy_year, 2),
        "Energy_Total": round(plant_sum_energy_total, 2),
        "Panel_Power": round(plant_sum_panel_power / 1000, 3),  # kW
        "Update_time": latest_update_time_str,
        "Server_Time": latest_server_time_str,
    }
    _LOGGER.debug("Aggregator: Aggregated plant data: %s", aggregated_data)
    return aggregated_data


def calculate_peak_power(
    current_power: float | None,
    previous_peak: float,
    last_reset_date: date | None,
) -> tuple[float, date, bool]:
    """
    Calculates the new peak power in kW, handling daily reset.

    current_power **deve estar em kW**, na mesma unidade de `aggregate_plant_data`.
    """
    now = datetime.now()
    current_date = now.date()
    state_changed = False
    new_peak = previous_peak
    new_reset_date = last_reset_date

    # Reset peak power if it's a new day
    if new_reset_date is None or new_reset_date != current_date:
        _LOGGER.info(
            f"Peak Power: New day ({current_date}) detected. Resetting peak from {new_peak:.3f} to 0.0 kW."
        )
        new_peak = 0.0
        new_reset_date = current_date
        state_changed = True

    # Update peak power if current power is higher
    if current_power is not None and current_power > new_peak:
        updated_peak = round(current_power, 3)
        _LOGGER.debug(
            f"Peak Power: New peak detected: {updated_peak:.3f} kW (Previous: {new_peak:.3f} kW)"
        )
        new_peak = updated_peak
        state_changed = True

    return new_peak, new_reset_date, state_changed
