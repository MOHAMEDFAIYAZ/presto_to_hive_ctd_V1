import logging
import re


# Function to extract the sap function along with nested functions into one variable
def getindex(startindex, expression):
    count = 0
    rcount = 0
    position = 0
    length = len(expression[:startindex])
    substring = expression[startindex:]

    ##print("substring===>",substring)
    # Extracts based on the number of ()
    for s in substring:
        if s == '(':
            count = count + 1
        elif s == ')':
            rcount = rcount + 1
        if count == rcount:
            break
        position = position + 1
    position = position + length
    return position


# Function to handle type casting
def typecasting(expression, source, target):
    a = expression.find(source)
    if source == 'isnull':
        expression = expression.replace(source, '', 1)
    else:
        expression = expression.replace(source, '(', 1)
    b = getindex(a, expression)
    startstr = expression[:b + 1]
    endstr = expression[b + 1:]
    return startstr + target + endstr


# Function to extract only the substring to be manipulated
def getparts(expression, func):
    # ##print(func)
    a = expression.find(func)
    startstr = expression[:a]
    endstr = expression[a:]
    # ##print(startstr)
    b = endstr.find("(")
    b = getindex(b, endstr)
    midstr = endstr[:b + 1]
    endstr = endstr[b + 1:]
    return startstr, midstr, endstr

#split_part(cast(last_day_of_month(DATE('2020-12-01')) as varchar),' ',1)
# in presto (timestamp) in hive gives date format
def last_day_of_month(type,expression):
    startstr, midstr, endstr = getparts(expression, type)
    print(midstr)
    midstr = midstr.replace(type,'LAST_DAY(')
    expression =startstr + midstr+endstr
    return expression
#regexp_like(ELEMENT_AT(dim_str, 'TrafficSourceCampaign'),'^[0-9]+\_[0-9]+$')

def regexp_like(type,expression):
    startstr, midstr, endstr = getparts(expression, type)
    midstr_split = midstr.replace('regexp_like(','').split(',')
    print(midstr_split)
    pattern  = midstr_split[-1].rstrip(')').strip("'")
    midstr_split_expression = midstr_split[:len(midstr_split)-1]
    col_expression = ','.join(midstr_split_expression)

    midstr ="{0} RLIKE '{1}'".format(col_expression,pattern)
    expression = startstr + midstr + endstr
    return expression

def element_at(type,expression):
    startstr, midstr, endstr = getparts(expression, type)
    midstr_split             = midstr.replace('element_at(','').split(',')
    object_name = midstr_split[0]
    key_name    = midstr_split[1].rstrip(')')
    midstr = "{0}[{1}]".format(object_name,key_name)
    expression = startstr+ midstr +endstr
    return expression


def strpos(type,expression):
    startstr, midstr, endstr = getparts(expression, type)
    midstr_split             = midstr.replace('strpos(','').split(',')
    search_string  = midstr_split[-1].rstrip(')')
    midstr_split_entexpression = midstr_split[:len(midstr_split)-1]
    entire_string = ','.join(midstr_split_entexpression)
    midstr = "LOCATE({0},{1})".format(search_string,entire_string)
    expression = startstr+midstr+endstr
    return expression

def split_part(type,expression):
    startstr, midstr, endstr = getparts(expression, type)
    midstr_split = midstr.replace('split_part(', '').split(',')
    print("split====>",midstr_split)
    split_index  = int(midstr_split[-1].rstrip(')'))-1
    midstr_split_entexpression = midstr_split[:len(midstr_split) - 1]
    entire_string = ','.join(midstr_split_entexpression)
    midstr = "SPLIT({0})[{1}]".format(entire_string,split_index)
    expression = startstr+midstr+endstr
    print(midstr)
    return expression

#date_format(date(a.report_date), '%Y-%m')

def date_format(type,expression):

    startstr, midstr, endstr = getparts(expression, type)
    midstr_split = midstr.replace('date_format(', '').split(',')
    split_index  = midstr_split[-1].rstrip(')').replace("%Y","YYYY").replace('%m','MM').replace('%d','DD')

    midstr_split_entexpression = midstr_split[:len(midstr_split) - 1]
    entire_string = ','.join(midstr_split_entexpression)
    midstr = "DATE_FORMAT({0},{1})".format(entire_string,split_index)
    expression = startstr+midstr+endstr
    return expression

    #   case when 1st parameter not RLIKE 2nd parameter and regexp_extract()='' then null
    #  when 1st parameter  RLIKE 2nd parameter
    # then regexp_extract as it is
    # END

def regexp_extract(type,expression):
    startstr, midstr, endstr = getparts(expression, type)

    midstr_split = midstr.replace('regexp_extract(', '').split(',')
    midstr = midstr.replace('regexp_extract(', 'REGEXP_EXTRACT(')
    split_index = midstr_split[-1].rstrip(')')
    midstr_split_entexpression = midstr_split[:len(midstr_split) - 1]
    print("midsr_regexp==>",midstr_split_entexpression)

    second_para = midstr_split_entexpression[-1]
    frst_para = "".join(midstr_split_entexpression[0:len(midstr_split_entexpression)-1])
    midstr = """case when {0} not RLIKE {1} and {2}='' 
then NULL        
when {0} RLIKE {1} 
then {2}
END
""".format(frst_para, second_para, midstr)
    expression = startstr+midstr+endstr
    return expression










# Function to map add_days,add_years etc to date_add
def date_add(type, expression, part):
    # ##print("add days")
    startstr, midstr, endstr = getparts(expression, type)
    # ##print(midstr)
    comma_pos = midstr.index(',')
    date_exp = midstr[:comma_pos]
    value = midstr[comma_pos + 1:len(midstr) - 1]
    date_exp = date_exp.replace(type, '')
    # ##print(date_exp+" - "+value)
    midstr = "DATEADD(" + part + "," + value + "," + date_exp + ")"
    expression = startstr + midstr + endstr
    return expression


def def_functionmapping(expression,json_data):

    errormsg = ''
    try:
        # funcArr contains list of complex functions to be mapped
        func_arr = ["regexp_like(","split_part(","date_format(","last_day_of_month(","regexp_extract(","element_at(",
                    "strpos("]
        expression = expression.replace("JOin", "JOIN").replace("Min", "MIN").replace(
            'in (', 'in(').replace('In (', 'IN(').replace('CURRENT_date', 'CURRENT_DATE'). \
            replace("\(", "frwd_slashopen").replace("\)", "frwd_slashclose").replace("'('", "single_quoteopen"). \
            replace("')'", "single_quoteclose")
        expression = expression.replace(' (', '(')

        for key, value in json_data.items():
            source = key + "("
            target = value + "("
            if source in expression:
                expression = expression.replace(source, target)
        # Change the functions in funcArr to lowercase
        for source in func_arr:
            if (source.upper() in expression.upper()):
                target = source.lower()
                expression = expression.replace(source.upper(), target)
            elif (source.lower() in expression.lower()):
                target = source.lower()
                expression = expression.replace(source.lower(), target)







        while True:
            if re.search(r"\blast_day_of_month\(\b",expression)!=None:#'last_day_of_month(' in expression :
                source = 'last_day_of_month('
                expression=last_day_of_month(source,expression)
            elif re.search(r"\bregexp_like\(\b",expression)!=None:
                source = 'regexp_like('
                expression=regexp_like(source,expression)
            elif re.search(r"\belement_at\(\b",expression)!=None:
                source = 'element_at('
                expression=element_at(source,expression)
            elif re.search(r"\bstrpos\(\b",expression)!=None:
                source = 'strpos('
                expression=strpos(source,expression)
            elif re.search(r"\bsplit_part\(\b",expression)!=None:
                source = 'split_part('
                expression=split_part(source,expression)
            elif re.search(r"\bdate_format\(\b",expression)!=None:
                source = 'date_format('
                expression = date_format(source, expression)
            elif re.search(r"\bregexp_extract\(\b",expression)!=None:
                source = 'regexp_extract('
                expression = regexp_extract(source, expression)

            else:
                break


    except Exception as e:
        # ##print(e)
        logging.exception("<p>Error: %s</p>" % e)
        errormsg = str(e).replace("\n", " - ")

    return expression, errormsg