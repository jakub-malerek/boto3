import pandas as pd
from dateutil import parser
import boto3
from io import BytesIO


def extract_data(file_name: str, bucket: str = "source-bucket-2024-eu") -> list[str]:
    """
    Extracts data from a file in an S3 bucket and returns it as a list of strings.

    Args:
        file_name (str): The name of the file to extract data from.
        bucket (str, optional): The name of the S3 bucket. Defaults to "source-bucket-2024-eu".

    Returns:
        list[str]: The extracted data as a list of strings.
    """
    s3 = boto3.resource("s3", region_name="eu-north-1", use_ssl=False)
    s3_bucket = s3.Bucket(bucket)

    file = BytesIO()
    s3_bucket.download_fileobj(Key=file_name, Fileobj=file)
    file.seek(0)

    lines_list = []

    for line in file:
        lines_list.append(line.decode("utf-8"))

    return lines_list


def clean_data(raw_text: list) -> pd.DataFrame:
    """
    Cleans the raw data and returns it as a pandas DataFrame.

    Args:
        raw_text (list): The raw data as a list of strings.

    Returns:
        pd.DataFrame: The cleaned data as a pandas DataFrame.
    """
    def standardize_dates(date_series):
        standardized_dates = []

        for date in date_series:
            try:
                parsed_date = parser.parse(date, dayfirst=True)
                standardized_dates.append(parsed_date.strftime('%Y-%m-%d'))
            except (ValueError, TypeError):
                standardized_dates.append("unknown")

        return pd.Series(standardized_dates)

    def extract_income(income_columns: list | tuple):
        data = income_columns.copy()
        for row in data:
            if row[4].replace("$", "").strip().isnumeric() and row[5].strip().isnumeric():
                row[4] = row[4] + row[5]
                row[5] = row[6]
                del row[6]
        return data

    def assume_subdomains(column):
        column = column.str.strip()
        domain_and_subdomain = column.str.split(
            "@").str[-1].apply(lambda x: x.split(".") if not x.endswith(".") else x[:-1].split("."))
        domain_and_subdomain = domain_and_subdomain
        emails_without_subdomain = domain_and_subdomain.str.len() == 1
        domain_labels = domain_and_subdomain[emails_without_subdomain]

        for label in domain_labels:
            label = label[0]
            domains_with_subdomain = domain_and_subdomain.apply(lambda x: label in x and len(x) > 1)
            most_frequent_subdomain = domain_and_subdomain[domains_with_subdomain].str[1].value_counts()
            if len(most_frequent_subdomain) > 0:
                most_frequent_subdomain = most_frequent_subdomain.index[0]
                rows_to_fill = emails_without_subdomain & (column.str.contains(label))
                column[rows_to_fill] = column[rows_to_fill].apply(
                    lambda x: (x + "." + most_frequent_subdomain)
                    if not x.endswith(".") else (x + most_frequent_subdomain))
            else:
                rows_to_fill = emails_without_subdomain & (column.str.contains(label))
                column[rows_to_fill] = column[rows_to_fill].apply(
                    lambda x: (x + "." + "com") if not x.endswith(".") else (x + "com"))

        return column

    # create columns and DataFrame out of raw rows of string
    data = [row.split(",") for row in raw_text]
    data_income_extracted = extract_income(data)
    data = pd.DataFrame(data_income_extracted[1:], columns=data_income_extracted[0])

    # clean income
    data["Income"] = data["Income"].str.replace("$", "")
    data["Income"] = data["Income"].apply(lambda x: x + ".00" if "." not in x else x)
    data["Income"] = data["Income"].replace({".00": "unknown"})

    # clean country
    data = data.rename(columns={"Country\n": "Country", "Country\r\n": "Country"})
    data["Country"] = data["Country"].str.replace("\n", "")
    country_usa_map = {"United States": "USA", "us": "USA", " U.S.": "USA", "usa": "USA",
                       "United States of America": "USA", "United states": "USA", "United Staes": "USA", "US": "USA"}
    country_usa_map2 = {
        "USA\r": "USA", "United States\r": "USA", "us\r": "USA", " U.S.\r": "USA", "usa\r": "USA",
        "United States of America\r": "USA", "United states\r": "USA", "United Staes\r": "USA", "US\r": "USA"}
    data["Country"] = data["Country"].replace(country_usa_map)
    data["Country"] = data["Country"].replace(country_usa_map2)

    # clean name
    data["Name"] = data["Name"].str.title()

    # clean email
    data["Email"] = data["Email"].str.replace("@@", "@")
    data["Email"] = assume_subdomains(data["Email"])
    data["Email"] = data["Email"].replace({".com": "unknown"})

    # clean date
    data["Date of Birth"] = standardize_dates(data["Date of Birth"])

    # clean age
    data["Age"] = data["Age"].replace("", "unknown")

    return data


def load_processed_data(data: pd.DataFrame, file_name: str, bucket: str, region_name: str = "eu-north-1") -> None:
    """
    Uploads the processed data to an S3 bucket.

    Args:
        data (pd.DataFrame): The processed data as a pandas DataFrame.
        file_name (str): The name of the file to save the data as.
        bucket (str): The name of the S3 bucket.
        region_name (str, optional): The AWS region name. Defaults to "eu-north-1".
    """
    csv_file = BytesIO()
    data.to_csv(csv_file, index=False)
    csv_file.seek(0)

    s3 = boto3.resource("s3", region_name=region_name, use_ssl=False)
    s3_bucket = s3.Bucket(bucket)

    s3_bucket.upload_fileobj(Fileobj=csv_file, Key=file_name)


if __name__ == "__main__":
    raw_data = extract_data("employee2.csv", bucket="source-bucket-2024-eu")

    processed_data = clean_data(raw_data)

    load_processed_data(data=processed_data, file_name="processed_employee2.csv",
                        bucket="bucket-2024-v4", region_name="us-east-1")
