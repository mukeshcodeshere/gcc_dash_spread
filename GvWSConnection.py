import requests
import datetime
import time
import urllib
from collections import OrderedDict


class GvException(Exception):
    def __init__(self, message, inner_exception=None):
        super().__init__(message)
        self.inner_exception = inner_exception


def _parse_num(val, fn):
    if val is None or len(val) == 0:
        return None

    try:
        ret = fn(val)
        return ret
    except:
        return None


def _parse_int(val):
    return _parse_num(val, int)


def _parse_float(val):
    return _parse_num(val, float)


def _parse_datetime(dt_str):
    try:
        ret = datetime.datetime.strptime(dt_str, "%m/%d/%Y %I:%M:%S %p")
        return ret
    except:
        return None


def _parse_date(dt_str):
    try:
        ret = datetime.datetime.strptime(dt_str, "%m/%d/%Y")
        return ret
    except:
        return _parse_datetime(dt_str)
    

class QuoteFields:
    symbol = 'pricesymbol'
    description = 'symboldescription'

    trade_date = 'tradedatetimeutc'
    open = 'open'
    high = 'high'
    low = 'low'
    close = 'close'  # settle
    last = 'last'
    mid_point = 'midpoint'
    volume = 'volume'
    trade_volume = 'tradevolume'
    historic_volume = 'historicvolume'
    tick_count = 'tickcount'

    net_change = 'netchange'
    percent_change = 'percentchange'
    open_interest = 'openinterest'

    close_date = 'closedate'
    currency = 'currency'
    most_recent_value = 'mostrecentvalue'
    most_recent_value_date = 'mostrecentvaluedate'
    last_trade_direction = 'lasttradedirection'

    previous_last = 'prevlast'
    previous_open = 'lastopen'
    previous_high = 'lasthigh'
    previous_low = 'lastlow'
    previous_close = 'lastclose'
    previous_volume = 'lastvolume'

    put_call_underlier = 'putcallunderlier'

    bid = 'bid'
    ask = 'ask'
    bid_size = 'bidsize'
    ask_size = 'asksize'
    bid_time = 'biddatetimeutc'
    ask_time = 'askdatetimeutc'

    option_root = 'optionroot'
    settle_date = 'settledate'
    contract_expiration_date = 'displaycontractexpdate'
    market = 'market'
    expiration_date = 'expirationdate'
    lot_unit = 'lotunit'
    strike = 'strike'

    trade_start_time = 'tradestarttimeutc'
    trade_stop_time = 'tradestoptimeutc'
    session_start_time = 'sessionstarttimeutc'
    session_stop_time = 'sessionstoptimeutc'
    block_trade_time = 'blocktradedatetimeutc'

    ALL = [symbol, description, trade_date, open, high, low, close, last, mid_point, volume, trade_volume,
           historic_volume, tick_count, net_change, percent_change, open_interest, close_date, currency,
           most_recent_value, most_recent_value_date, last_trade_direction, previous_last, previous_open, previous_high,
           previous_low, previous_close, previous_volume, put_call_underlier, bid, ask, bid_size, ask_size, bid_time,
           ask_time, option_root, settle_date, contract_expiration_date, market, expiration_date, lot_unit, strike,
           trade_start_time, trade_stop_time, session_start_time, session_stop_time, block_trade_time]

    STANDARD = [symbol, trade_date, open, high, low, last, volume, open_interest]


class TimeSeriesFields:
    symbol = "pricesymbol"
    trade_date = "tradedatetimeutc"
    open = "open"
    high = "high"
    low = "low"
    close = "close"
    volume = "volume"
    mid_point = "midpoint"
    open_interest = "openinterest"

    ALL = [symbol, trade_date, open, high, low, close, volume, mid_point, open_interest]
    INTRADAY = [symbol, trade_date, open, high, low, close, volume]


class FillMethod:
    FillForward = 0
    FillBackward = 1
    Average = 2
    Interpolate = 3
    NoFill = 4


class FillFrequency:
    Business = 0
    SixDays = 1
    SevenDays = 2


class AggregateType:
    Daily = 0
    Peak = 1
    OffPeak = 2


class ForwardCurveValueType:
    Price = 0
    Spread = 1
    SpreadPercent = 2
    Relative = 3
    RelativePercent = 4
    SpreadWave = 5
    SpreadPercentWave = 6
    Spread100PecentWave = 7
    SpreadAvg = 8


class LeadLagType:
    CalendarDays = 0,
    QuotedDays = 1,
    Weeks = 2


class LeadLagOptions:
    def __init__(self, ll_type, ll_amount):
        if ll_type < LeadLagType.CalendarDays or ll_type > LeadLagType.Weeks:
            raise ValueError("Invalid type")

        if ll_amount is None:
            raise ValueError("Amount is missing")

        self.lead_lag_type = ll_type
        self.lead_lag_amount = ll_amount


_fields_conversion = {
    QuoteFields.trade_date: _parse_date,

    QuoteFields.open: _parse_float,
    QuoteFields.high: _parse_float,
    QuoteFields.low: _parse_float,
    QuoteFields.close: _parse_float,
    QuoteFields.last: _parse_float,
    QuoteFields.mid_point: _parse_float,
    QuoteFields.volume: _parse_int,
    QuoteFields.trade_volume: _parse_int,
    QuoteFields.historic_volume: _parse_int,
    QuoteFields.tick_count: _parse_int,

    QuoteFields.net_change: _parse_float,
    QuoteFields.percent_change: _parse_float,
    QuoteFields.open_interest: _parse_int,

    QuoteFields.close_date: _parse_date,
    QuoteFields.most_recent_value: _parse_float,
    QuoteFields.most_recent_value_date: _parse_date,
    QuoteFields.last_trade_direction: _parse_float,

    QuoteFields.previous_last: _parse_float,
    QuoteFields.previous_open: _parse_float,
    QuoteFields.previous_high: _parse_float,
    QuoteFields.previous_low: _parse_float,
    QuoteFields.previous_close: _parse_float,
    QuoteFields.previous_volume: _parse_int,

    QuoteFields.bid: _parse_float,
    QuoteFields.ask: _parse_float,
    QuoteFields.bid_size: _parse_int,
    QuoteFields.ask_size: _parse_int,
    QuoteFields.bid_time: _parse_date,
    QuoteFields.ask_time: _parse_date,

    QuoteFields.settle_date: _parse_date,
    QuoteFields.contract_expiration_date: _parse_date,
    QuoteFields.expiration_date: _parse_date,
    QuoteFields.strike: _parse_float,

    QuoteFields.trade_start_time: _parse_date,
    QuoteFields.trade_stop_time: _parse_date,
    QuoteFields.session_start_time: _parse_date,
    QuoteFields.session_stop_time: _parse_date,
    QuoteFields.block_trade_time: _parse_date,
}

_fields_names_helper = {
    'symbol': 'pricesymbol',
    'description': 'symboldescription',
    
    'trade_date': 'tradedatetimeutc',
    'open': 'open',
    'high': 'high',
    'low': 'low',
    'close': 'close',
    'settle': 'close',
    'last': 'last',
    'mid_point': 'midpoint',
    'volume': 'volume',
    'trade_volume': 'tradevolume',
    'historic_volume': 'historicvolume',
    'tick_count': 'tickcount',
    
    'net_change': 'netchange',
    'percent_change': 'percentchange',
    'open_interest': 'openinterest',
    
    'close_date': 'closedate',
    'currency': 'currency',
    'most_recent_value': 'mostrecentvalue',
    'most_recent_value_date': 'mostrecentvaluedate',
    'last_trade_direction': 'lasttradedirection',
    
    'previous_last': 'prevlast',
    'previous_open': 'lastopen',
    'previous_high': 'lasthigh',
    'previous_low': 'lastlow',
    'previous_close': 'lastclose',
    'previous_volume': 'lastvolume',
    
    'put_call_underlier': 'putcallunderlier',
    
    'bid': 'bid',
    'ask': 'ask',
    'bid_size': 'bidsize',
    'ask_size': 'asksize',
    'bid_time': 'biddatetimeutc',
    'ask_time': 'askdatetimeutc',
    
    'option_root': 'optionroot',
    'settle_date': 'settledate',
    'contract_expiration_date': 'displaycontractexpdate',
    'market': 'market',
    'expiration_date': 'expirationdate',
    'lot_unit': 'lotunit',
    'strike': 'strike',
    
    'trade_start_time': 'tradestarttimeutc',
    'trade_stop_time': 'tradestoptimeutc',
    'session_start_time': 'sessionstarttimeutc',
    'session_stop_time': 'sessionstoptimeutc',
    'block_trade_time': 'blocktradedatetimeutc'
}


def _time_to_local_time(utc_datetime):
    epoch = time.mktime(utc_datetime.timetuple())
    offset = datetime.datetime.fromtimestamp(epoch) - datetime.datetime.utcfromtimestamp(epoch)
    return utc_datetime + offset


class GviResult(OrderedDict):
    """
    Represents one row in returned data set. Individual fields can be addresed as row['field_name'] or row.field_name 
    """

    def __getattr__(self, key):
        try:
            # first try with given key verbatim
            ret = self.get(key, None)
            if ret is not None:
                return ret

            # try to find the value by converting the key to lowercase
            low_key = key.lower()
            ret = self.get(low_key, None)
            if ret is not None:
                return ret

            # as the last resort, try to find key name replacement
            r_key = _fields_names_helper.get(low_key, None)
            if r_key is not None:
                return self[r_key]

            # just give up and let the base class to deal with the key
            return self[key]

        except KeyError:
            # to conform with __getattr__ spec
            raise AttributeError(key)

    def __init__(self, header_fields, value_fields, convert_to_local_time=False, *args, **kwds):
        super().__init__(*args, **kwds)
        self.field_names = header_fields
        for i in range(len(header_fields)):
            header_name = header_fields[i].lower()
            field_value = value_fields[i]

            # if field_value is None:
            #     continue

            fn = _fields_conversion.get(header_name, None)
            if fn is not None:
                val = fn(field_value)
                if convert_to_local_time and isinstance(val, datetime.datetime):
                    val = _time_to_local_time(val)

                self[header_name] = val
            else:
                self[header_name] = field_value


class Units:
    BBL = "BBL"  # Barrels
    LTR = "LTR"  # Liters
    KLTR = "KLTR"  # Kiloliters
    CM = "CM"  # Cubic Meters
    GAL = "GAL"  # Gallons
    MSCF = "MSCF"  # Thou/Std Cubic Ft.
    MT = "MT"  # Metric Tons
    ST = "ST"  # Short Tons
    MMB = "MMB"  # MMBTUs
    THM = "THM"  # Therms
    GJ = "GJ"  # Gigajoules
    GWH = "GWH"  # Gigawatt Hours
    KWH = "KWH"  # Kilowatt Hours
    MWH = "MWH"  # Megawatt Hours


class UnitConversion:
    def __init__(self, unit, factor=None):
        """
        Unit conversion for the symbol
        :param unit: See Units class for available units
        :param factor: Conversion factor (optional)
        """
        self.unit = unit
        self.factor = factor


class Currencies:
    USD = "USD"  # U.S. Dollar
    USC = "USC"  # U.S. Cents
    GBP = "GBP"  # British Pound
    GBC = "GBC"  # British Pence
    EUR = "EUR"  # Euro
    EUC = "EUC"  # Euro Cents
    JPY = "JPY"  # Janapese Yen
    CNV = "CNV"  # Chinese Yuan
    CHF = "CHF"  # Swiss Franc
    SGD = "SGD"  # Singapore Dollar
    CAD = "CAD"  # Canadian Dollar
    CAC = "CAC"  # Canadian Cents
    AUD = "AUD"  # Australian Dollar
    NZD = "NZD"  # New Zealand Dollar
    MYR = "MYR"  # Malaysian Ringgit
    HKD = "HKD"  # Hong Kong Dollar
    KRW = "KRW"  # South Korean Won
    THB = "THB"  # Thai Baht
    DKK = "DKK"  # Danish Krone
    NOK = "NOK"  # Norwegian Kroner
    SEK = "SEK"  # Swedish Krona
    TWD = "TWD"  # Taiwan Dollar
    ZAR = "ZAR"  # South African Rand
    BRL = "BRL"  # Brazilian Real
    MXN = "MXN"  # Mexican Peso
    BHD = "BHD"  # Bahrain Dinar
    SAR = "SAR"  # Saudi Arabian Riyal
    RUB = "RUB"  # Russian Ruble
    ARS = "ARS"  # Argentine Peso


class CurrencySources:
    BCB = "BCB"  # Bank of Brasil
    BNM = "BNM"  # Bank of Malaysia
    BNZ = "BNZ"  # Bank of New Zealand
    BOC = "BOC"  # Bank of Canada
    BOE = "BOE"  # Bank of England
    BOI = "BOI"  # Bank of Indonesia
    BOJ = "BOJ"  # Bank of Tokyo
    BOM = "BOM"  # Bank of Mexico
    BOT = "BOT"  # Bank of Thailand
    DNB = "DNB"  # Danish National Bank
    ECB = "ECB"  # European Central Bank
    FXZ = "FXZ"  # Bank of China
    IMF = "IMF"  # Int. Monetary Fund
    RBA = "RBA"  # Reserve Bank of Australia
    USF = "USF"  # US Fed. Reserve


class ConvertedSymbol:
    """
    Represents symbol if currency/unit conversion is needed.
    """

    def __init__(self, symbol, currency=None, currency_source=None, unit=None, unit_factor=None):
        """
        Represent symbol which prices should be converted
        :param symbol: Symbol name (string)
        :param currency: Currency name (string) - see Currencies class for a list of available currencies 
        :param currency_source: Exchange name (string) - see CurrencySources class for a list of sources 
        :param unit: unit name (string) - see Units class
        :param unit_factor: unit conversion factor (float, optional) 
        """
        if symbol is None:
            raise ValueError("Symbol can't be undefined")

        symbol = urllib.parse.quote_plus(symbol)

        if len(currency or "") == 0: 
            currency = None
            
        if len(currency_source or "") == 0: 
            currency_source = None
            
        if len(unit or "") == 0: 
            unit = None
            
        if unit_factor is not None and unit_factor == 0:
            unit_factor = None

        def format_part(first, second):
            if first is not None:
                ret_val = "@{}".format(first)
                if second is not None:
                    ret_val = ret_val + ":" + str(second)

                return ret_val
            else:
                return None

        def format_formula(first, second):
            if first is None and second is None:
                return None

            if first is None:
                return second

            if second is None:
                return first

            return first + ',' + second

        currency_part = format_part(currency, currency_source)
        unit_part = format_part(unit, unit_factor)
        formula_part = format_formula(currency_part, unit_part)

        if formula_part is None:
            self.formula = symbol
            self.is_formula = False
        else:
            self.formula = '=""{}"";[{}]'.format(symbol, formula_part)
            self.is_formula = True

    def __str__(self):
        return self.formula


class GvWSConnection:
    """
    Encapsulates web service calls
    """

    def __init__(self, username, password):
        self.username = username
        self.password = password

        self._url_base = 'http://webservice.gvsi.com/gvsi/query/htsv/'
        self._quote_rq = 'GetQuotes/{}?{}'
        self._timeseries_rq = 'GetDaily/{}?{}'
        self._intraday_rq = 'GetIntraday/{}?{}'
        self._curve_rq = 'GetForwardCurve/{}?{}'

    def _fetch_data(self, url):
        # query_string = self._url_base + url
        result = requests.get(url, auth=(self.username, self.password))
        txt = result.text

        if result.status_code != 200:
            error_text = txt
            if error_text is None or len(error_text) == 0:
                error_text = "HTTP error, code: {}".format(result.status_code)

            raise GvException(error_text)

        lines = txt.splitlines()
        return lines

    @staticmethod
    def _process_table_data(lines, convert_to_local_time=False):
        if len(lines) == 1:
            # it's either empty response, or an error
            fields = lines[0].split('\t')
            if len(fields) > 1: # we'll assume this is a regular, empty server response
                values = [None] * len(fields)
                res = GviResult(fields, values, False)
                return [res]

        if len(lines) < 2:
            msg = "Invalid server response"
            if len(lines) == 1:
                msg += ": " + lines[0]

            raise GvException(msg)

        header_line = lines[0]
        header_fields = header_line.split('\t')

        ret_array = []
        for result_line in lines[1:]:
            result_fields = result_line.split('\t')
            if len(result_fields) != len(header_fields):
                continue

            row_obj = GviResult(header_fields, result_fields, convert_to_local_time)
            ret_array.append(row_obj)

        return ret_array

    def _prepare_query(self, query_preffix, symbols, fields, symbol_field_name='pricesymbol', check_for_symbol=None):
        if symbols is None: 
            raise ValueError("Symbol(s) missing")
        
        if fields is None: 
            raise ValueError("Fields missing")

        if isinstance(symbols, str) or isinstance(symbols, ConvertedSymbol):
            symbols_list = [symbols]
        else:
            symbols_list = symbols

        escaped_symbols = []
        for symbol in symbols_list:
            if isinstance(symbol, str):
                escaped = urllib.parse.quote_plus(symbol)
                escaped_symbols.append(escaped)
            else:
                escaped_symbols.append(symbol)

        quoted_symbols = ['{}="{}"'.format(symbol_field_name, x) for x in escaped_symbols]
        arg_symbols = "|".join(quoted_symbols)

        if check_for_symbol is not None and TimeSeriesFields.symbol not in fields:
            fields.insert(0, TimeSeriesFields.symbol)

        arg_fields = "/".join(fields)

        query_string = self._url_base + query_preffix.format(arg_fields, arg_symbols)
        return query_string

    class TsEnum:
        days = 'days'
        weeks = 'weeks'
        months = 'months'
        contract_months = 'contractmonths'
        quarters = 'quarters'
        years = 'years'
        intraday = 'intraday'

        ALL = [days, weeks, months, contract_months, quarters, years, intraday]

    def _get_timeseries(self, bar_interval, symbols, fields, grouped, *,
                        start_date=None, end_date=None, num_of_bars=None,
                        fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                        lead_lag_options=None, iso_hour_selection=None,
                        intraday_interval=None, intraday_local_time=False):

        if symbols is None: 
            raise ValueError("Symbol(s) missing")
        
        if fields is None: 
            raise ValueError("Fileds missing")
        
        if bar_interval not in self.TsEnum.ALL:
            raise ValueError("Invalid interval")
        
        if bar_interval == self.TsEnum.intraday:
            if intraday_interval is None:
                raise ValueError("Undefined intraday interval")
            
            base_query = self._intraday_rq
            period_query = '&intradaybarinterval={}&queryversion=3'.format(intraday_interval)
            process_times = intraday_local_time
        else:
            base_query = self._timeseries_rq
            period_query = '&dailybarinterval=' + bar_interval
            process_times = False
        
        if start_date is None and end_date is None and num_of_bars is None:
            raise ValueError("Either start_date and stop_date or days_back must be specified")

        freq = [FillFrequency.SixDays, FillFrequency.SevenDays, FillFrequency.Business]

        if fill_frequency is not None and fill_frequency not in freq:
            raise ValueError("Invalid fill_frequency")

        aggregate = [AggregateType.Daily, AggregateType.Peak, AggregateType.OffPeak]
        if iso_hour_selection is not None and iso_hour_selection not in aggregate:
            raise ValueError("Invalid iso_hour_selection")

        query_string = self._prepare_query(base_query, symbols, fields, check_for_symbol=grouped)

        if num_of_bars is not None:
            query_string += "&daysback={}".format(num_of_bars)

        time_format = '%Y/%m/%d'
        if start_date is not None:
            dt = start_date.strftime(time_format)
            query_string += '&chartstartdate=' + dt

        if end_date is not None:
            dt = end_date.strftime(time_format)
            query_string += '&chartstopdate=' + dt

        query_string += period_query

        if fill_method is not None and fill_method != FillMethod.NoFill:
            ff = fill_frequency
            if ff is None:
                ff = FillFrequency.SevenDays

            query_string += "&fillmethod={}&fillfrequency={}".format(fill_method, ff)

        if lead_lag_options is not None:
            ll = "&leadlagperiods={}&leadlagperiodtype={}".format(
                lead_lag_options.lead_lag_amount,
                lead_lag_options.lead_lag_period)
            query_string += ll

        if iso_hour_selection is not None:
            h = '&normalizemethod="1"&aggregatetype="{}"'.format(iso_hour_selection)
            query_string += h

        ret = self._fetch_data(query_string)
        lines = self._process_table_data(ret, process_times)

        if not grouped:
            return lines

        groups = {}
        for row in lines:
            symbol = row.symbol
            group = groups.get(symbol, None)
            if group is None:
                group = []
                groups[symbol] = group
            group.append(row)

        return groups

    def get_quote(self, symbols, fields=QuoteFields.STANDARD):
        """ Get quotes.
        
        :param symbols: individual symbol name or list of symbol names. Symbol name can be either a string,
                        or instance of ConvertedSymbol class, if conversion is needed
        :param fields: list of field names. See QuoteFields class.
        :return: list of GviResult objects, each representing quote data for one symbol
        """

        query = self._prepare_query(self._quote_rq, symbols, fields)
        data = self._fetch_data(query)
        ret_list = self._process_table_data(data)
        return ret_list

    def get_daily(self, symbols, fields=TimeSeriesFields.ALL, *, grouped=False,
                  start_date=None, end_date=None, num_of_bars=None,
                  fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                  lead_lag_options=None, iso_hour_selection=None):

        """ Get daily prices.
        
        :param symbols: individual symbol name or list of symbol names. Symbol name can be either a string,
                        or instance of ConvertedSymbol class, if conversion is needed
        :param fields: list of field names. See TimeSeriesFields class.
        :param grouped: should the function return flat list of results (grouped=False) or dictionary of symbol names
                        to list of results for particular symbol.
        :param start_date: optional start date of the period (date)
        :param end_date:  optional end date of the period (date)
        :param num_of_bars: optional number of bars should be returned, 
                          counting from end_date or today if end_date is None. (date)
                          
                          The request must have either start_date-end_date, end_date-days_back or num_of_bars specified.
                          
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param lead_lag_options: object of LeadLagOptions class
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options
        :return: depends of grouped param - either list of GviResult objects, each representing one bar,
                           or dictionary of symbol names to GviResult bars lists
        """

        return self._get_timeseries(self.TsEnum.days, symbols, fields, grouped,
                                    start_date=start_date, end_date=end_date, num_of_bars=num_of_bars,
                                    fill_method=fill_method, fill_frequency=fill_frequency,
                                    lead_lag_options=lead_lag_options, iso_hour_selection=iso_hour_selection)

    def get_daily_tail(self, symbol, bars, lead_lag_options=None, currency=None, currency_source=None, conversion=None,
                       fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                       iso_hour_selection=None):

        """ Get latest daily prices.
        
        :param symbol: Symbol name (string) 
        :param bars: number of daily bars that should be returned (int).
        :param lead_lag_options: object of LeadLagOptions class. 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class.
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options.
        :return: list of GviResult objects, each representing one bar. 
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        fields = TimeSeriesFields.ALL
        return self._get_timeseries(self.TsEnum.days, sym, fields, grouped=False, num_of_bars=bars,
                                    lead_lag_options=lead_lag_options, fill_method=fill_method,
                                    fill_frequency=fill_frequency, iso_hour_selection=iso_hour_selection)

    def get_daily_range(self, symbol, from_date, to_date, lead_lag_options=None, currency=None, currency_source=None,
                        conversion=None, fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                        iso_hour_selection=None):

        """ Get daily prices for given date range
        
        :param symbol: Symbol name (string) 
        :param from_date: start of the period (date)
        :param to_date: end of the period (date)
        :param lead_lag_options: object of LeadLagOptions class 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options
        :return: list of GviResult objects, each representing one bar. 
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        fields = TimeSeriesFields.ALL
        return self._get_timeseries(self.TsEnum.days, sym, fields, grouped=False, start_date=from_date,
                                    end_date=to_date, lead_lag_options=lead_lag_options, fill_method=fill_method,
                                    fill_frequency=fill_frequency, iso_hour_selection=iso_hour_selection)

    def get_weekly(self, symbols, fields=TimeSeriesFields.ALL, *, grouped=False,
                   start_date=None, end_date=None, num_of_bars=None,
                   fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                   lead_lag_options=None, iso_hour_selection=None):

        """ Get weekly prices.

        :param symbols: individual symbol name or list of symbol names. Symbol name can be either a string,
                        or instance of ConvertedSymbol class, if conversion is needed
        :param fields: list of field names. See TimeSeriesFields class.
        :param grouped: should the function return flat list of results (grouped=False) or dictionary of symbol names
                        to list of results for particular symbol.
        :param start_date: optional start date of the period (date)
        :param end_date:  optional end date of the period (date)
        :param num_of_bars: optional number of bars should be returned, 
                          counting from end_date or today if end_date is None. (date)

                          The request must have either start_date-end_date, end_date-days_back or num_of_bars specified.

        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param lead_lag_options: object of LeadLagOptions class
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options
        :return: depends of grouped param - either list of GviResult objects, each representing one bar,
                           or dictionary of symbol names to GviResult bars lists
        """

        return self._get_timeseries(self.TsEnum.weeks, symbols, fields, grouped,
                                    start_date=start_date, end_date=end_date, num_of_bars=num_of_bars,
                                    fill_method=fill_method, fill_frequency=fill_frequency,
                                    lead_lag_options=lead_lag_options, iso_hour_selection=iso_hour_selection)

    def get_weekly_tail(self, symbol, bars, lead_lag_options=None, currency=None, currency_source=None, conversion=None,
                        fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                        iso_hour_selection=None):

        """ Get latest weekly prices.

        :param symbol: Symbol name (string) 
        :param bars: number of daily bars that should be returned (int).
        :param lead_lag_options: object of LeadLagOptions class. 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class.
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options.
        :return: list of GviResult objects, each representing one bar. 
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        fields = TimeSeriesFields.ALL
        return self._get_timeseries(self.TsEnum.weeks, sym, fields, grouped=False, num_of_bars=bars,
                                    lead_lag_options=lead_lag_options, fill_method=fill_method,
                                    fill_frequency=fill_frequency, iso_hour_selection=iso_hour_selection)

    def get_weekly_range(self, symbol, from_date, to_date, lead_lag_options=None, currency=None, currency_source=None,
                         conversion=None, fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                         iso_hour_selection=None):

        """ Get weekly prices for given date range

        :param symbol: Symbol name (string) 
        :param from_date: start of the period (date)
        :param to_date: end of the period (date)
        :param lead_lag_options: object of LeadLagOptions class 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options
        :return: list of GviResult objects, each representing one bar. 
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        fields = TimeSeriesFields.ALL
        return self._get_timeseries(self.TsEnum.weeks, sym, fields, grouped=False, start_date=from_date,
                                    end_date=to_date, lead_lag_options=lead_lag_options, fill_method=fill_method,
                                    fill_frequency=fill_frequency, iso_hour_selection=iso_hour_selection)

    def get_monthly(self, symbols, fields=TimeSeriesFields.ALL, *, grouped=False,
                    start_date=None, end_date=None, num_of_bars=None,
                    fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                    lead_lag_options=None, iso_hour_selection=None):

        """ Get monthly prices.

        :param symbols: individual symbol name or list of symbol names. Symbol name can be either a string,
                        or instance of ConvertedSymbol class, if conversion is needed
        :param fields: list of field names. See TimeSeriesFields class.
        :param grouped: should the function return flat list of results (grouped=False) or dictionary of symbol names
                        to list of results for particular symbol.
        :param start_date: optional start date of the period (date)
        :param end_date:  optional end date of the period (date)
        :param num_of_bars: optional number of bars should be returned, 
                          counting from end_date or today if end_date is None. (date)

                          The request must have either start_date-end_date, end_date-days_back or num_of_bars specified.

        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param lead_lag_options: object of LeadLagOptions class
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options
        :return: depends of grouped param - either list of GviResult objects, each representing one bar,
                           or dictionary of symbol names to GviResult bars lists
        """

        return self._get_timeseries(self.TsEnum.months, symbols, fields, grouped,
                                    start_date=start_date, end_date=end_date, num_of_bars=num_of_bars,
                                    fill_method=fill_method, fill_frequency=fill_frequency,
                                    lead_lag_options=lead_lag_options, iso_hour_selection=iso_hour_selection)

    def get_monthly_tail(self, symbol, bars, lead_lag_options=None, currency=None, currency_source=None,
                         conversion=None, fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                         iso_hour_selection=None):

        """ Get latest monthly prices.

        :param symbol: Symbol name (string) 
        :param bars: number of daily bars that should be returned (int).
        :param lead_lag_options: object of LeadLagOptions class. 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class.
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options.
        :return: list of GviResult objects, each representing one bar. 
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        fields = TimeSeriesFields.ALL
        return self._get_timeseries(self.TsEnum.months, sym, fields, grouped=False, num_of_bars=bars,
                                    lead_lag_options=lead_lag_options, fill_method=fill_method,
                                    fill_frequency=fill_frequency, iso_hour_selection=iso_hour_selection)

    def get_monthly_range(self, symbol, from_date, to_date, lead_lag_options=None, currency=None, currency_source=None,
                          conversion=None, fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                          iso_hour_selection=None):

        """ Get monthly prices for given date range

        :param symbol: Symbol name (string) 
        :param from_date: start of the period (date)
        :param to_date: end of the period (date)
        :param lead_lag_options: object of LeadLagOptions class 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options
        :return: list of GviResult objects, each representing one bar. 
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        fields = TimeSeriesFields.ALL
        return self._get_timeseries(self.TsEnum.months, sym, fields, grouped=False, start_date=from_date,
                                    end_date=to_date, lead_lag_options=lead_lag_options, fill_method=fill_method,
                                    fill_frequency=fill_frequency, iso_hour_selection=iso_hour_selection)

    def get_quarterly(self, symbols, fields=TimeSeriesFields.ALL, *, grouped=False,
                      start_date=None, end_date=None, num_of_bars=None,
                      fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                      lead_lag_options=None, iso_hour_selection=None):

        """ Get quarterly prices.

        :param symbols: individual symbol name or list of symbol names. Symbol name can be either a string,
                        or instance of ConvertedSymbol class, if conversion is needed
        :param fields: list of field names. See TimeSeriesFields class.
        :param grouped: should the function return flat list of results (grouped=False) or dictionary of symbol names
                        to list of results for particular symbol.
        :param start_date: optional start date of the period (date)
        :param end_date:  optional end date of the period (date)
        :param num_of_bars: optional number of bars should be returned, 
                          counting from end_date or today if end_date is None. (date)

                          The request must have either start_date-end_date, end_date-days_back or num_of_bars specified.

        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param lead_lag_options: object of LeadLagOptions class
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options
        :return: depends of grouped param - either list of GviResult objects, each representing one bar,
                           or dictionary of symbol names to GviResult bars lists
        """

        return self._get_timeseries(self.TsEnum.quarters, symbols, fields, grouped,
                                    start_date=start_date, end_date=end_date, num_of_bars=num_of_bars,
                                    fill_method=fill_method, fill_frequency=fill_frequency,
                                    lead_lag_options=lead_lag_options, iso_hour_selection=iso_hour_selection)

    def get_quarterly_tail(self, symbol, bars, lead_lag_options=None, currency=None, currency_source=None,
                           conversion=None, fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                           iso_hour_selection=None):

        """ Get latest quarterly prices.

        :param symbol: Symbol name (string) 
        :param bars: number of daily bars that should be returned (int).
        :param lead_lag_options: object of LeadLagOptions class. 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class.
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options.
        :return: list of GviResult objects, each representing one bar. 
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        fields = TimeSeriesFields.ALL
        return self._get_timeseries(self.TsEnum.quarters, sym, fields, grouped=False, num_of_bars=bars,
                                    lead_lag_options=lead_lag_options, fill_method=fill_method,
                                    fill_frequency=fill_frequency, iso_hour_selection=iso_hour_selection)

    def get_quarterly_range(self, symbol, from_date, to_date, lead_lag_options=None, currency=None, currency_source=None,
                            conversion=None, fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                            iso_hour_selection=None):

        """ Gets quarterly prices for given date range

        :param symbol: Symbol name (string) 
        :param from_date: start of the period (date)
        :param to_date: end of the period (date)
        :param lead_lag_options: object of LeadLagOptions class 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options
        :return: list of GviResult objects, each representing one bar. 
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        fields = TimeSeriesFields.ALL
        return self._get_timeseries(self.TsEnum.quarters, sym, fields, grouped=False, start_date=from_date,
                                    end_date=to_date, lead_lag_options=lead_lag_options, fill_method=fill_method,
                                    fill_frequency=fill_frequency, iso_hour_selection=iso_hour_selection)

    def get_yearly(self, symbols, fields=TimeSeriesFields.ALL, *, grouped=False,
                   start_date=None, end_date=None, num_of_bars=None,
                   fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                   lead_lag_options=None, iso_hour_selection=None):

        """ Get yearly prices.

        :param symbols: individual symbol name or list of symbol names. Symbol name can be either a string,
                        or instance of ConvertedSymbol class, if conversion is needed
        :param fields: list of field names. See TimeSeriesFields class.
        :param grouped: should the function return flat list of results (grouped=False) or dictionary of symbol names
                        to list of results for particular symbol.
        :param start_date: optional start date of the period (date)
        :param end_date:  optional end date of the period (date)
        :param num_of_bars: optional number of bars should be returned, 
                          counting from end_date or today if end_date is None. (date)

                          The request must have either start_date-end_date, end_date-days_back or num_of_bars specified.

        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param lead_lag_options: object of LeadLagOptions class
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options
        :return: depends of grouped param - either list of GviResult objects, each representing one bar,
                           or dictionary of symbol names to GviResult bars lists
        """

        return self._get_timeseries(self.TsEnum.years, symbols, fields, grouped,
                                    start_date=start_date, end_date=end_date, num_of_bars=num_of_bars,
                                    fill_method=fill_method, fill_frequency=fill_frequency,
                                    lead_lag_options=lead_lag_options, iso_hour_selection=iso_hour_selection)

    def get_yearly_tail(self, symbol, bars, lead_lag_options=None, currency=None, currency_source=None, conversion=None,
                        fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                        iso_hour_selection=None):

        """ Get latest yearly prices.

        :param symbol: Symbol name (string) 
        :param bars: number of daily bars that should be returned (int).
        :param lead_lag_options: object of LeadLagOptions class. 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class.
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options.
        :return: list of GviResult objects, each representing one bar. 
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        fields = TimeSeriesFields.ALL
        return self._get_timeseries(self.TsEnum.years, sym, fields, grouped=False, num_of_bars=bars,
                                    lead_lag_options=lead_lag_options, fill_method=fill_method,
                                    fill_frequency=fill_frequency, iso_hour_selection=iso_hour_selection)

    def get_yearly_range(self, symbol, from_date, to_date, lead_lag_options=None, currency=None, currency_source=None,
                         conversion=None, fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                         iso_hour_selection=None):

        """ Get yearly prices for given date range

        :param symbol: Symbol name (string) 
        :param from_date: start of the period (date)
        :param to_date: end of the period (date)
        :param lead_lag_options: object of LeadLagOptions class 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options
        :return: list of GviResult objects, each representing one bar. 
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        fields = TimeSeriesFields.ALL
        return self._get_timeseries(self.TsEnum.years, sym, fields, grouped=False, start_date=from_date,
                                    end_date=to_date, lead_lag_options=lead_lag_options, fill_method=fill_method,
                                    fill_frequency=fill_frequency, iso_hour_selection=iso_hour_selection)

    def get_intraday(self, symbols, fields=TimeSeriesFields.INTRADAY, bar_interval=5, *, grouped=False,
                     start_date=None, end_date=None, days_back=None, use_local_time=False,
                     fill_method=FillMethod.NoFill, fill_frequency=FillFrequency.SevenDays,
                     lead_lag_options=None, iso_hour_selection=None):

        """ Get intraday prices.
        
        :param symbols: individual symbol name or list of symbol names. Symbol name can be either a string,
                        or instance of ConvertedSymbol class, if conversion is needed
        :param fields: list of field names. See TimeSeriesFields class.
        :param bar_interval: intraday bar interval (int). 
        :param grouped: should the function return flat list of results (grouped=False) or dictionary of symbol names
                        to list of results for particular symbol.
        :param start_date: optional start date of the period (date)
        :param end_date:  optional end date of the period (date)
        :param days_back: optional number of days back should be returned, 
                          counting from end_date or today if end_date is None. (date)
                          
                          The request must have either start_date-end_date, end_date-days_back or num_of_bars specified.
                          
        :param use_local_time: convert bar time to local time (bool) 
        :param fill_method: how to fill missing bars. See FillMethod class, and fill_frequency param.
        :param fill_frequency: how missing days should be filled. See FillFrequency class. 
        :param lead_lag_options: object of LeadLagOptions class
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options
        :return: depends of grouped param - either list of GviResult objects, each representing one bar,
                           or dictionary of symbol names to GviResult bars lists
        """
        
        return self._get_timeseries(self.TsEnum.intraday, symbols, fields, grouped,
                                    start_date=start_date, end_date=end_date, num_of_bars=days_back,
                                    fill_method=fill_method, fill_frequency=fill_frequency,
                                    lead_lag_options=lead_lag_options, iso_hour_selection=iso_hour_selection,
                                    intraday_interval=bar_interval, intraday_local_time=use_local_time)

    def get_intraday_tail(self, symbol, days, minutes, use_local_time=False, fields=TimeSeriesFields.INTRADAY,
                          currency=None, currency_source=None, conversion=None, iso_hour_selection=None):

        """ Get latest intraday prices
        
        :param symbol: Symbol name (string) 
        :param days: number of days to display (int) 
        :param minutes: intraday bar interval (int)
        :param use_local_time: convert bar's time to local time (bool) 
        :param fields: list of field names. See TimeSeriesFields class. 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class.
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options.
        :return: list of GviResult objects, each representing one intraday bar
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        return self._get_timeseries(self.TsEnum.intraday, sym, fields, grouped=False, num_of_bars=days,
                                    intraday_interval=minutes, intraday_local_time=use_local_time,
                                    iso_hour_selection=iso_hour_selection)

    def get_intraday_range(self, symbol, from_date, to_date, minutes, use_local_time=False,
                           fields=TimeSeriesFields.INTRADAY, currency=None, currency_source=None, conversion=None,
                           iso_hour_selection=None):

        """
        
        :param symbol: Symbol name (string)
        :param from_date: start of the period (date)
        :param to_date: end of the period (date)
        :param minutes: intraday bar interval (int)
        :param use_local_time: convert bar's time to local time (bool) 
        :param fields: list of field names. See TimeSeriesFields class. 
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class.
        :param iso_hour_selection: ISO hour selection. See AggregateType class for available options.
        :return: list of GviResult objects, each representing one intraday bar
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        return self._get_timeseries(self.TsEnum.intraday, sym, fields, grouped=False, start_date=from_date,
                                    end_date=to_date, intraday_interval=minutes, intraday_local_time=use_local_time,
                                    iso_hour_selection=iso_hour_selection)

    def get_curve(self, roots, fields=TimeSeriesFields.ALL, curve_date=None, curve_type=ForwardCurveValueType.Price,
                  grouped=False):

        """ Get forward curve
        
        :param roots: individual root symbol name or list of root symbol names. Root symbol name can be either a string,
                        or instance of ConvertedSymbol class, if conversion is needed
        :param fields: list of field names. See TimeSeriesFields class.
        :param curve_date: Forward curve date (date)
        :param curve_type: See ForwardCurveValueType class for list of valid types
        :param grouped: should the function return flat list of results (grouped=False) or dictionary of symbol names
                        to list of results for particular symbol.
        :return: depends of grouped param - either list of GviResult objects, each representing one symbol,
                           or dictionary of symbol names to GviResult lists
        """

        if roots is None:
            raise ValueError("Root(s) missing")

        if fields is None:
            raise ValueError("Fields missing")

        if curve_type < ForwardCurveValueType.Price or curve_type > ForwardCurveValueType.SpreadAvg:
            raise ValueError("Invalid curve type")

        query_string = self._prepare_query(self._curve_rq, roots, fields, symbol_field_name='curveroot',
                                           check_for_symbol=True)

        if curve_date is not None:
            dt = curve_date.strftime('%Y/%m/%d')
            query_string += "&curvedate2=" + dt

        query_string += "&curvevaluetype={}".format(curve_type)

        ret = self._fetch_data(query_string)
        lines = self._process_table_data(ret)

        if not grouped:
            return lines

        groups = {}
        for row in lines:
            symbol = row.symbol
            group = groups.get(symbol, None)
            if group is None:
                group = []
                groups[symbol] = group
            group.append(row)

        return groups

    def get_forward_curve(self, symbol, curve_date=None, curve_type=ForwardCurveValueType.Price,
                          currency=None, currency_source=None, conversion=None):

        """ Get forward curve
        
        :param symbol: root symbol name (string)
        :param curve_date: Forward curve date (date)
        :param curve_type: See ForwardCurveValueType class for list of valid types.
        :param currency: currency name. See Currencies class for reference.        
        :param currency_source: currency source. See CurrencySources class.
        :param conversion: object of UnitConversion class.
        :return: list of GviResult objects, each representing one symbol 
        """

        if conversion:
            unit = conversion.unit
            factor = conversion.factor
        else:
            unit = None
            factor = None

        sym = ConvertedSymbol(symbol, currency=currency, currency_source=currency_source, unit=unit, unit_factor=factor)
        fields = TimeSeriesFields.ALL
        return self.get_curve(sym, fields, curve_date=curve_date, curve_type=curve_type)
