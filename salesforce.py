import requests
from BeautifulSoup import BeautifulSoup as Soup
from datetime import datetime
import time
import base64
import sqlite3
from StringIO import StringIO
import zipfile
import binascii

##### BASE CLASSES #####

class SFDCSoapRequest(object):

    def __init__(self):
        self._skeleton = """
            <Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
                <Header>
                    %(header)s
                </Header>
                <Body>
                    %(body)s
                </Body>
            </Envelope>
        """
        self._soap_headers = [];
        self._soap_body = [];

        debug_header = """
        <DebuggingHeader xmlns="http://soap.sforce.com/2006/08/apex">
            <debugLevel>Detail</debugLevel>
        </DebuggingHeader>
        """
        self.add_soap_header(debug_header)

        self._url = 'https://login.salesforce.com/services/Soap/c/28'
        self._url_args = {}

    def make_request(self):
        url = self._url % self._url_args

        payload = self._skeleton % {'header': '\n'.join(x for x in self._soap_headers),
                                    'body': '\n'.join(x for x in self._soap_body)}

        headers = {'SOAPAction': '""', 'Content-Type': 'text/xml'}

        response = requests.post(url = url,
                                 data = payload,
                                 headers = headers)

        soup = Soup(response.text)
        return self._response_callback(soup)

    def _response_callback(self, response):
        pass

    def add_soap_header(self, header_str):
        self._soap_headers.append(header_str)

    def add_soap_body(self, body_str):
        self._soap_body.append(body_str)

class AuthenticatedSFDCSoapRequest(SFDCSoapRequest):

    def __init__(self, server_instance, session_id, session_ns):
        super(AuthenticatedSFDCSoapRequest, self).__init__()

        session_header = """
        <SessionHeader xmlns="%(sessionNS)s">
            <sessionId>%(sessionId)s</sessionId>
        </SessionHeader>
        """ % {'sessionId': session_id,
               'sessionNS': session_ns}

        self.add_soap_header(session_header)

        self._url = 'https://%(server)s.salesforce.com/services/Soap/%(endpoint)s/%(version)s/%(org_id)s' 
        self._url_args = {
            'version': '28.0', 
            'org_id': session_id.split('!')[0],
            'server': server_instance,
        }

class MetadataSFDCSoapRequest(AuthenticatedSFDCSoapRequest):
    def __init__(self, server_instance, session_id):
        session_ns = 'http://soap.sforce.com/2006/04/metadata'
        super(MetadataSFDCSoapRequest, self).__init__(server_instance, session_id, session_ns)
        self._url_args['endpoint'] = 'm'

class ApexSFDCSoapRequest(AuthenticatedSFDCSoapRequest):
    def __init__(self, server_instance, session_id):
        session_ns = 'http://soap.sforce.com/2006/08/apex'
        super(ApexSFDCSoapRequest, self).__init__(server_instance, session_id, session_ns)
        self._url_args['endpoint'] = 's'

class EnterpriseSFDCSoapRequest(AuthenticatedSFDCSoapRequest):
    def __init__(self, session_id):
        session_ns = 'http://soap.sforce.com/2006/08/apex'
        super(MetadataSFDCSoapRequest, self).__init__(server_instance, session_id, session_ns)
        self._url_args['endpoint'] = 'e'

##### SPECIFIC CLASSES #####

class LoginSFDCRequest(SFDCSoapRequest):
    def __init__(self, username, password, security_token):
        super(LoginSFDCRequest, self).__init__()

        login_body = """
            <login xmlns="urn:enterprise.soap.sforce.com">
                <username>%(username)s</username>
                <password>%(password)s%(security_token)s</password>
            </login>
        """ % {
            'username': username, 
            'password': password,
            'security_token': security_token}

        self.add_soap_body(login_body)

    def _response_callback(self, response):
        data = {}

        data['session_id'] = response.find('sessionid').getText()

        metadata_url = response.find('metadataserverurl').getText()
        data['server_instance'] = metadata_url.split('https://')[1].split('.')[0]

        return data

class ExecuteAnonymousSFDCRequest(ApexSFDCSoapRequest):
    def __init__(self, server_instance, session_id, text):
        super(ExecuteAnonymousSFDCRequest, self).__init__(server_instance, session_id)

        ea_body = """
        <executeAnonymous xmlns="http://soap.sforce.com/2006/08/apex">
            <String>%(text)s</String>
        </executeAnonymous>
        """ % {
            'text': text,
        }

        self.add_soap_body(ea_body)

    def _response_callback(self, response):
        data = {}

        data['debug_log'] = response.find('debuglog').getText()
        data['compile_problem'] = response.find('compileproblem').getText()
        data['was_compiled'] = response.find('compiled').getText() == 'true'

        return data

class ListMetadataSFDCRequest(MetadataSFDCSoapRequest):
    def __init__(self, server_instance, session_id, metadata_type):
        super(ListMetadataSFDCRequest, self).__init__(server_instance, session_id)

        rm_body = """
        <listMetadata xmlns="http://soap.sforce.com/2006/04/metadata">
            <queries>
                <type>%(type)s</type>
            </queries>
            <asOfVersion>%(version)s</asOfVersion>
        </listMetadata>
        """ % {'type': metadata_type,
                'version': '28.0'}

        self.add_soap_body(rm_body)

    def _response_callback(self, response):
        data = []

        dt_format = '%Y-%m-%dT%H:%M:%S.000Z'

        for result in response.findAll('result'):
            d = {}
            d['id'] = result.find('id').getText()
            d['filename'] = result.find('filename').getText()
            d['created_date'] = datetime.strptime(result.find('createddate').getText(), dt_format)
            d['last_modified_date'] = datetime.strptime(result.find('lastmodifieddate').getText(), dt_format)
            data.append(d)

        return data

class RetrieveSFDCRequest(MetadataSFDCSoapRequest):
    def __init__(self, server_instance, session_id):
        super(RetrieveSFDCRequest, self).__init__(server_instance, session_id)

        r_body = """
        <retrieve xmlns="http://soap.sforce.com/2006/04/metadata">
            <retrieveRequest>
                <apiVersion>28.0</apiVersion>
                <singlePackage>false</singlePackage>
                <unpackaged>
                    <types>
                        <members>*</members>
                        <name>ApexClass</name>
                    </types>
                    <types>
                        <members>*</members>
                        <name>ApexComponent</name>
                    </types>
                    <types>
                        <members>*</members>
                        <name>ApexPage</name>
                    </types>
                    <types>
                        <members>*</members>
                        <name>ApexTrigger</name>
                    </types>
                    <types>
                        <members>*</members>
                        <name>CustomObject</name>
                    </types>
                    <types>
                        <members>*</members>
                        <name>StaticResource</name>
                    </types>
                <version>28.0</version>
                </unpackaged>
            </retrieveRequest>
        </retrieve>
        """
        self.add_soap_body(r_body)

    def _response_callback(self, response):
        return response.find('id').getText()

class CheckRetrieveSFDCRequest(MetadataSFDCSoapRequest):
    def __init__(self, server_instance, session_id, retrieve_id):
        super(CheckRetrieveSFDCRequest, self).__init__(server_instance, session_id)

        r_body = """
        <checkRetrieveStatus xmlns="http://soap.sforce.com/2006/04/metadata">
            <asyncProcessId>%(retrieve_id)s</asyncProcessId>
        </checkRetrieveStatus>
        """ % {'retrieve_id': retrieve_id}

        self.add_soap_body(r_body)

    def _response_callback(self, response):
        if response.find('faultstring'):
            return {'success': False}
        else:
            zip_data = response.find('zipfile').getText()
            return {'success': True, 'zip': str( zip_data ) }

class SFDCRequestHandler(object):
    def __init__(self):
        self._session_id = None
        self._server_instance = None

        self._is_logged_in = False

        self._project_components = ['ApexClass', 'ApexComponent', 'ApexPage', 'ApexTrigger', 'StaticResource', 'CustomObject']

    def login(self, username, password, security_token):
        self._username = username
        self._password = password
        self._security_token = security_token

        login_data = LoginSFDCRequest(username, password, security_token).make_request()

        self._session_id = login_data['session_id']
        self._server_instance = login_data['server_instance']

    def execute_anonymous(text):
        if not self._is_logged_in:
            raise Exception('You need to login first.')

        ea_data = ExecuteAnonymousSFDCRequest(self._server_instance, self._session_id, text).make_request()

        return ea_data

    def list_metadata(self):
        for component in self._project_components:
            metadata = ListMetadataSFDCRequest(self._server_instance, self._session_id, component).make_request()

    def _get_remote_zip(self):
        retrieve_id = RetrieveSFDCRequest(self._server_instance, self._session_id).make_request()
        zip_data = CheckRetrieveSFDCRequest(self._server_instance, self._session_id, retrieve_id).make_request()

        while not zip_data['success']:
            zip_data = CheckRetrieveSFDCRequest(self._server_instance, self._session_id, retrieve_id).make_request()
            time.sleep(2)

        return self._str_to_zip(zip_data['zip'])

    def _str_to_zip(self, s):
        return zipfile.ZipFile((StringIO(base64.b64decode(s))))

    def _zip_to_str(self, z):
        return base64.b64encode(z.fp.buf)

    def synchronize(self, method):

        zip_file = self._get_remote_zip()

        updated_files = zipfile.ZipFile(StringIO(''), 'w')

        for remote_file in zip_file.filelist:
            remote_filename = remote_file.filename
            local_filename = 'src/'+remote_filename[11:]
            with open(local_filename, 'r') as local_file:
                local_buffer = local_file.read()
                local_crc = (binascii.crc32(local_buffer) & 0xFFFFFFFF)
                remote_crc = remote_file.CRC

                if remote_crc != local_crc:
                    updated_files.write(local_filename, remote_filename)

                else:
                    updated_files.writestr(remote_file)

        updated_files_str = _zip_to_str(updated_files)
