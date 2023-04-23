import logging
import csv
import xml.etree.ElementTree as ET
import boto3


logging.basicConfig(filename='conversion.log', level=logging.INFO)


class XMLToCSVConverter:
    def __init__(self, xml_file_path, bucket_name, s3_file_path):
        self.xml_file_path = xml_file_path
        self.tree = None
        self.root = None
        self.data = []
        self.headers = [
            'FinInstrmGnlAttrbts.Id',
            'FinInstrmGnlAttrbts.FullNm',
            'FinInstrmGnlAttrbts.ClssfctnTp',
            'FinInstrmGnlAttrbts.CmmdtyDerivInd',
            'FinInstrmGnlAttrbts.NtnlCcy',
            'Issr'
        ]
        self.bucket_name = bucket_name
        self.s3_file_path = s3_file_path

    def parse_xml(self):
        try:
            self.tree = ET.parse(self.xml_file_path)
            self.root = self.tree.getroot()
            logging.info('XML file parsed successfully')
        except Exception as e:
            logging.error(f'Error parsing XML file: {e}')

    def extract_data(self):
        for child in self.root:
            data_row = []
            for sub_child in child.iter():
                if sub_child.tag in self.headers:
                    data_row.append(sub_child.text)
            self.data.append(data_row)

    def write_to_csv(self):
        with open('output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(self.headers)
            writer.writerows(self.data)
            logging.info('Data written to CSV file successfully')

    def upload_to_s3(self):
        s3 = boto3.resource('s3')
        try:
            s3.meta.client.upload_file('assignment.csv', self.bucket_name, self.s3_file_path)
            logging.info('CSV file uploaded to S3 bucket successfully')
        except Exception as e:
            logging.error(f'Error uploading CSV file to S3 bucket: {e}')

    def convert(self):
        self.parse_xml()
        self.extract_data()
        self.write_to_csv()
        self.upload_to_s3()


if __name__ == '__main__':
    converter = XMLToCSVConverter('DLTINS_20210117_01of01.xml', 'my-bucket', 'output.csv')
    converter.convert()
