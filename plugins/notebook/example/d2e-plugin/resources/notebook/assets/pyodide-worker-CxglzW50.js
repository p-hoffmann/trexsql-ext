var u=`"""
Module \`pyqe\` is the python interface to QE. The goal of
this module is to help researchers to build rule-based cohorts for
further analysis.
"""

from pyqe.api.query import Query
from pyqe.api.pa_config import PAConfig
from pyqe.api.result import Result
from pyqe.api.cohort import Cohort
from pyqe.api.concept_query import ConceptQuery, ConceptSet, Concept
from pyqe.api.concept_set_query import ConceptSetQuery
from pyqe.api.datasource import DataSource
from pyqe.ql.person import Person
from pyqe.ql.interaction import Interactions
from pyqe.ql.criteria_group import CriteriaGroup
from pyqe.ql.attribute import Attribute, Constraint, DateConstraint, Expression
from pyqe.ql.advanced_time_filter import AdvanceTimeFilter
from pyqe.ql.date_period import DatePeriod, CurrentDatePeriod
from pyqe.types.enum_types import ConfigPath, CardType, LogicalOperator, ComparisonOperator, MatchCriteria, Domain, Format, OriginSelection, TargetSelection

__all__ = [
    # pyqe.api.query
    'Query',
    # pyqe.api.pa_config
    'PAConfig',
    # pyqe.api.result
    'Result',
    # pyqe.api.cohort
    'Cohort',
    # pyqe.api.concept_query
    'ConceptQuery',
    'ConceptSetQuery',
    'ConceptSet',
    'Concept',
    # pyqe.api.datasource
    'DataSource',
    # pyqe.ql.filter_card
    'Person',
    'Interactions',
    # pyqe.ql.criteria_group
    'CriteriaGroup',
    # pyqe.ql.attribute
    'Attribute',
    'Constraint',
    'DateConstraint',
    'Expression',
    # pyqe.ql.advanceTimeFilter
    'AdvanceTimeFilter',
    # pyqe.ql.date_period
    'DatePeriod',
    'CurrentDatePeriod',
    # pyqe.types.enum_types
    'ConfigPath',
    'CardType',
    'LogicalOperator',
    'ComparisonOperator',
    'MatchCriteria',
    'Domain',
    'Format',
    'OriginSelection',
    'TargetSelection'
]
`,f=`"""
About
-------------
\`pyqe.api\` submodule of \`pyqe\` contains all api classes
"""
`,m=`import os
import json
import logging
import requests
import jwt
import getpass
from urllib.parse import urljoin
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv
from six.moves import input
from typing import Optional, Any, List
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator, settings
from pyqe.azure.password_grant import _PasswordCredential
from pyqe.azure.refresh_token import _RefreshToken
from pyodide.http import pyfetch, FetchResponse

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class _StarboardApi():
    def __init__(self):
        """Api class

        Provide common functions used for accessing QE endpoints
        """

        self._load_environment_variables()

    async def _get(self, path: str, params=None, **kwargs) -> FetchResponse:
        """Request HTTP GET method"""
        if params:
            query_string = '?'
            for k, v in params.items():
                if query_string != '?':
                    query_string += '&'
                query_string += k + '=' + v
            path += query_string
            print(path)

        url = urljoin(str(self._base_url), str(path))
        logger.debug(f'GET {url}')
        headers = {"Authorization": f'Bearer {os.getenv("TOKEN")}'}
        response = await pyfetch(url, method="GET", headers=headers, **kwargs)
        return response

    async def _post(self, path: str, data=None, **kwargs) -> FetchResponse:
        """Request HTTP POST method"""

        url = urljoin(str(self._base_url), str(path))
        logger.debug(f'POST {url}')
        headers = {
            "Authorization": f'Bearer {os.getenv("TOKEN")}', "Content-Type": "application/json"}

        response = await pyfetch(url, method="POST", body=json.dumps(data), headers=headers, **kwargs)
        return response

    async def _put(self, path: str, data=None) -> FetchResponse:
        """Request HTTP PUT method"""

        url = urljoin(str(self._base_url), str(path))
        logger.debug(f'PUT {url}')
        headers = {"Authorization": f'Bearer {os.getenv("TOKEN")}'}
        response = await pyfetch(
            url, method="PUT", body=json.dumps(data), headers=headers)
        return response

    async def _delete(self, path: str, **kwargs) -> FetchResponse:
        """Request HTTP DELETE method"""

        url = urljoin(str(self._base_url), str(path))
        logger.debug(f'DELETE {url}')
        headers = {"Authorization": f'Bearer {os.getenv("TOKEN")}'}
        response = await pyfetch(url, method="DELETE", headers=headers, **kwargs)
        return response

    def _load_environment_variables(self) -> None:
        """Load relevant environment variables"""
        load_dotenv()

        self._base_url = os.getenv('PYQE_URL')
        self._pyqe_tls_ca_cert_path = os.getenv('PYQE_TLS_CLIENT_CA_CERT_PATH')

        if self._base_url is None:
            raise ValueError('Please set PYQE_URL in .env')

        if self._pyqe_tls_ca_cert_path is None:
            raise ValueError('Please set PYQE_TLS_CLIENT_CA_CERT_PATH in .env')

        self._connect_timeout: Optional[str] = os.getenv('PYQE_CONNECT_TIMEOUT')
        self._read_timeout: Optional[str] = os.getenv('PYQE_READ_TIMEOUT')

        if all(val is None for val in [self._connect_timeout, self._read_timeout]):
            self._timeout = None
        else:
            self._timeout = (float(self._connect_timeout), float(self._read_timeout))


@decorator.attach_class_decorator(decorator.log_function, __name__)
class _AuthApi(_StarboardApi):
    def __init__(self):
        """Provide common functions used for accessing protected WebAPI endpoints"""

        super().__init__()
        self._load_environment_variables()
        self._username: Optional[str] = None

        self._auth_types = {
            '0': 'None'
        }

    def _load_environment_variables(self) -> None:
        super()._load_environment_variables()
        self._default_auth_type: Optional[str] = os.getenv('PYQE_AUTH_TYPE')
        self._auth_audience: Optional[str] = os.getenv('PYQE_JWT_AUDIENCE')
        self._auth_algorithms: list = []
        _auth_algorithms: Optional[str] = os.getenv('PYQE_JWT_ALGORITHMS')
        if _auth_algorithms:
            self._auth_algorithms = _auth_algorithms.split()

        if os.getenv('PYQE_TOKEN_TYPE'):
            self._pyqe_token_type = os.getenv('PYQE_TOKEN_TYPE')
        else:
            self._pyqe_token_type = 'ACCESS'

    @property
    def id_token(self) -> Optional[str]:
        return os.getenv('OIDC_ID_TOKEN')

    @property
    def access_token(self) -> Optional[str]:
        return os.getenv('OIDC_ACCESS_TOKEN')

    @property
    def refresh_token(self) -> Optional[str]:
        return os.getenv('OIDC_REFRESH_TOKEN')

    @property
    def is_auth_disabled(self) -> bool:
        return os.getenv('PYQE_AUTH_TYPE') == '0'

    @property
    def auth_type(self):
        return os.getenv('PYQE_AUTH_TYPE')

    @property
    def has_id_token(self):
        return True if self.id_token else False

    def get_id(self):
        return self._decode_id_token(os.getenv('TOKEN'))['sub']

    def _decode_id_token(self, token):
        decode_kwargs = {
            'options': {'verify_signature': False, 'verify_exp': True}
        }

        if self._auth_audience is not None:
            decode_kwargs['audience'] = self._auth_audience

        if self._auth_algorithms is not None:
            decode_kwargs['algorithms'] = self._auth_algorithms

        return jwt.decode(token, **decode_kwargs)

    def _create_authorization_header(self):
        if self._pyqe_token_type == 'ACCESS':
            return {'Authorization': f'Bearer {self.access_token}'}
        else:
            return {'Authorization': f'Bearer {self.id_token}'}

    async def _get(self, path: str, params=None, **kwargs):
        try:
            response = await super()._get(path, params=params, **kwargs)
            return response
        except requests.HTTPError as e:
            self._validate_response(e.response)
            return e.response
        except Exception as e:
            print(e)
            raise

    async def _post(self, path: str, data=None, **kwargs):
        try:
            response = await super()._post(path, data=data, **kwargs)
            return response
        except requests.HTTPError as e:
            self._validate_response(e.response)
            return e.response
        except Exception as e:
            print(e)
            raise

    async def _put(self, path: str, data=None):
        try:
            response = await super()._put(path, data=data)
            return response
        except requests.HTTPError as e:
            self._validate_response(e.response)
            return e.response
        except Exception as e:
            print(e)
            raise

    async def _delete(self, path: str, **kwargs):
        try:
            response = await super()._delete(path, **kwargs)
            return response
        except requests.HTTPError as e:
            self._validate_response(e.response)
            return e.response
        except Exception as e:
            print(e)
            raise

    def _validate_response(self, response: requests.Response) -> None:
        url = response.request.url
        method = response.request.method

        if response.status_code == 401:
            logger.error(f'Anonymous access is not allowed ({method} {url})')
            self._reauthentication()
            return
        elif response.status_code == 403:
            raise PermissionError(f'Access is not permitted ({method} {url})')

        response.raise_for_status()
`,g=`import json
import logging
import os
from pyqe.api.base import _AuthApi
from pyqe.shared import decorator
from pyqe.shared.b64encode_query import _EncodeQueryStringMixin
from pyqe.setup import setup_simple_console_log

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class Cohort(_EncodeQueryStringMixin, _AuthApi):
    """Cohort class that allows:
        - retrieve list of cohorts
        - create cohort
        - delete cohort
    Args:
        study_id: String value defining the study id
    """

    def __init__(self, study_id: str = None):
        super().__init__()
        if not study_id:
            if os.environ['PYQE_STUDY_ENTITY_VALUE']:
                self.study_id = os.environ['PYQE_STUDY_ENTITY_VALUE']
            else:
                raise ValueError('Please specify a study id\\n')
        else:
            self.study_id = study_id

    async def get_all_cohorts(self, limit: int = 0, offset: int = 0) -> dict:
        params = {
            "datasetId": self.study_id,
            "offset": str(offset),
            "limit": str(limit)
        }
        response = await self._get('/analytics-svc/api/services/cohort', params)
        return await response.json()

    async def delete_cohort(self, cohort_id: int) -> bool:
        response = await self._delete(
            f'/analytics-svc/api/services/cohort?cohortId={cohort_id}&datasetId={self.study_id}')
        return response.string()

    async def create_cohort(self, cohort_definition: dict) -> str:

        if os.getenv('KERNAL') == 'JUPYTER':
            cohort_definition['mriquery'] = str(
                self._encode_query_string(cohort_definition['mriquery']), 'utf-8')
        else:
            cohort_definition['mriquery'] = str(self._encode_query_string(
                cohort_definition['mriquery']).decode('ascii'))

        cohort_definition['datasetId'] = self.study_id
        cohort_definition['syntax'] = json.dumps(cohort_definition['syntax'])

        response = await self._post('/analytics-svc/api/services/cohort', json=cohort_definition)
        return response.string()
`,h=`import logging
import json
from typing import List, Optional
from pyqe.api.base import _AuthApi
from pyqe.types.enum_types import Domain
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class ConceptQuery(_AuthApi):
    """Query client for OMOP concept functions"""

    def __init__(self):
        super().__init__()

    def get_standard_code(self, concept_code: str, vocabulary_id: str) -> str:
        """Get standard concept code based on provided non-standard concept code & vocabulary ID

        Args:
            concept_code: String value of non-standard concept code

            vocabulary_id: String value of vocabulary id of provided non-standard concept code
        """
        params = {
            'conceptCode': concept_code,
            'vocabularyId': vocabulary_id
        }
        response = self._get(f'/analytics-svc/api/services/standard-concept', params=params)
        concept = json.loads(response.text)
        return concept['conceptCode']

    def get_descendant_codes(self, concept_code: str) -> List[str]:
        """Get list of descendant concept codes based on provided concept code

        Args:
            concept_code: String value of concept code
        """
        params = {
            'conceptCode': concept_code
        }
        response = self._get(f'/analytics-svc/api/services/descendant-concepts', params=params)
        json_response = json.loads(response.text)
        concepts = json_response['descendants']
        if len(concepts) > 0:
            return [concept['conceptCode'] for concept in concepts]
        else:
            raise ValueError('No descendant concept code found')

    async def get_standard_concept_ids(self, concept_code: str, vocabulary_id: str = '') -> str:
        """Get standard concept id based on provided non-standard concept code & vocabulary ID

        Args:
            concept_code: String value of non-standard concept code

            vocabulary_id: String value of vocabulary id of provided non-standard concept code
        """
        params = {
            'conceptCode': concept_code,
            'vocabularyId': vocabulary_id
        }

        response = await self._get('/analytics-svc/api/services/standard-concept-ids', params)
        res = await response.json()
        concept_id_list = res['concept_id']
        return concept_id_list

@decorator.attach_class_decorator(decorator.log_function, __name__)
class ConceptSet:
    """Defined set of unique concept codes for a domain which can be included in :py:class:\`FilterCard <pyqe.ql.filter_card.FilterCard>\`

        Args:
            name: String value defining the concept set name

            domain: Domain value

            concept_codes: optional list of concept codes

            concepts: optional list of :py:class:\`Concept <pyqe.api.concept_query.Concept>\`
    """

    def __init__(self, name: str, domain: Domain, concept_codes: List[str] = None,
                 concepts: List['Concept'] = None):
        concept_codes = self._initialise_list(concept_codes)
        concepts = self._initialise_list(concepts)
        self.name: str = name
        self.domain: Domain = domain
        self.concept_codes: List[str] = concept_codes
        self.excluded_concept_codes: List[str] = []
        for concept in concepts:
            self.add_concept(concept)
        # TODO: Add verification of concept code

    def add_concept_code(self, concept_code: str):
        """Add OMOP concept code in the ConceptSet

        Args:
            concept_code: String value of concept code

        Raises:
            ValueError: An error occurred if provided concept_code is already excluded
        """
        if concept_code in self.excluded_concept_codes:
            raise ValueError(f'Concept code {concept_code} is excluded from ConceptSet')
        else:
            self.concept_codes.append(concept_code)

    def add_concept(self, concept: 'Concept'):
        """Add OMOP concept and its descendant concept codes in the ConceptSet

        Args:
            concept: OMOP Concept
        """
        self.concept_codes.append(concept.concept_code)
        if concept.include_descendants is True:
            descendant_codes = ConceptQuery().get_descendant_codes(concept.concept_code)
            for descendant_code in descendant_codes:
                if descendant_code in self.excluded_concept_codes:
                    descendant_codes.remove(descendant_code)
                    logger.info(
                        f'Excluded descendant concept code {descendant_code} found and not added in ConceptSet')
            self.concept_codes = self._merge_lists(self.concept_codes, descendant_codes)

    def exclude_concept_code(self, excluded_concept_code: str):
        """Exclude OMOP concept code in the ConceptSet

        Args:
            concept_code: String value of concept code to be excluded
        """
        self.excluded_concept_codes.append(excluded_concept_code)
        self._remove_excluded_concept_code(excluded_concept_code)

    def exclude_concept_codes(self, concept_codes: List[str]):
        """Exclude list of OMOP concept codes in the ConceptSet

        Args:
            concept_codes: list of concept codes to be excluded
        """
        self.excluded_concept_codes = self._merge_lists(self.excluded_concept_codes, concept_codes)
        for excluded_concept_code in concept_codes:
            self._remove_excluded_concept_code(excluded_concept_code)

    def exclude_concept_ids(self, concept_ids: List[str]):
        """Exclude list of OMOP concept ids in the ConceptSet

        Args:
            concept_codes: list of concept ids to be excluded
        """
        self.excluded_concept_codes = self._merge_lists(self.excluded_concept_codes, concept_ids)
        for excluded_concept_id in concept_ids:
            self._remove_excluded_concept_code(excluded_concept_id)

    def _remove_excluded_concept_code(self, excluded_concept_code):
        if excluded_concept_code in self.concept_codes:
            self.concept_codes.remove(excluded_concept_code)
            logger.info(
                f'Concept code {excluded_concept_code} found and excluded from ConceptSet')

    def _is_concept_code_excluded(self, concept_code: str) -> bool:
        return concept_code in self.excluded_concept_codes

    def _merge_lists(self, first_list: List[str], second_list: List[str]) -> bool:
        new_values = set(second_list) - set(first_list)
        return first_list + list(new_values)

    def _initialise_list(self, _list: Optional[List]) -> List:
        if _list is None:
            _list = []
        return _list


@decorator.attach_class_decorator(decorator.log_function, __name__)
class Concept:
    """OMOP concept and its descendant concept codes

        Args:
            concept_code: concept code of Concept

            include_descendants: boolean value to include descendants or not
    """

    def __init__(self, concept_code: str, include_descendants: bool = True):
        self.concept_code = concept_code
        self.include_descendants = include_descendants
`,y=`import os
import logging
import json
import pandas as pd
from typing import List, Dict
from pyqe.types.types import ConceptSet, ConceptSetConcept
from pyqe.api.base import _AuthApi
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class ConceptSetQuery(_AuthApi):
    """Query client for OMOP concept set functions"""

    def __init__(self, study_id: str):
        super().__init__()
        self.concept_sets = pd.DataFrame()
        if not study_id:
            if os.environ["PYQE_STUDY_ENTITY_VALUE"]:
                self.study_id = os.environ["PYQE_STUDY_ENTITY_VALUE"]
            else:
                raise ValueError("Please specify a study id\\n")
        else:
            self.study_id = study_id

    async def get_all_concept_sets(self) -> List[ConceptSet]:
        """Query all concept sets

        Args:
            None
        """
        params = {
            "datasetId": self.study_id,
        }

        response = await self._get(f"/terminology/concept-set", params=params)
        json_response: List[ConceptSet] = await response.json()

        # Cache concept_set in class variable
        self.concept_sets = pd.json_normalize(json_response)

        return json_response

    def show_concept_set_list(self):
        """Prints id and name for all concept sets

        Args:
            None
        """
        if len(self.concept_sets) == 0:
            print("No concept sets found!")
            return

        concept_sets_for_display = "Concept Set ID - Concept Set Name"
        for index, concept_set in self.concept_sets.iterrows():
            concept_sets_for_display += (
                f'\\n({ index + 1 }) { concept_set["id"] } - { concept_set["name"] }'
            )

        print(concept_sets_for_display)

    def get_concept_set_from_id(self, concept_set_id: int) -> ConceptSet | Dict:
        """
        Get concept set from concept set id

        Args:
            concept_set_id: ID of concept set
        """
        if len(self.concept_sets) == 0:
            return {}

        concept_set_concepts = self.concept_sets[
            self.concept_sets["id"] == concept_set_id
        ]

        if len(concept_set_concepts) == 0:
            return {}

        return json.loads(concept_set_concepts.iloc[0].to_json())

    def get_concept_set_ids_from_name(self, concept_set_name: int) -> list[int]:
        """
        Get concept set ids from name
        Return value is an array of int as multiple concept sets can have the same name

        Args:
            concept_set_name: Name of concept set
        """
        if len(self.concept_sets) == 0:
            return []

        concept_set_ids = list(
            self.concept_sets[self.concept_sets["name"] == concept_set_name]["id"]
        )

        return concept_set_ids

    def get_concepts_in_concept_set(
        self, concept_set_id: int
    ) -> list[ConceptSetConcept]:
        """
        Get concept set concepts from concept set id

        Args:
            concept_set_id: ID of concept set
        """
        if len(self.concept_sets) == 0:
            return []

        concept_set_concepts = self.concept_sets[
            self.concept_sets["id"] == concept_set_id
        ]

        if len(concept_set_concepts) == 0:
            return []

        return concept_set_concepts.iloc[0]["concepts"]
`,v=`import json
import logging
from pyqe.api.base import _AuthApi
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator
from typing import Optional

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class DataSource(_AuthApi):
    def __init__(self):
        super().__init__()

    # Get list of table from a given schema
    def get_table_names(self, schemaName: str):
        response = self._get("/analytics-svc/api/services/customDBs/{}".format(schemaName), {})
        return json.loads(response.text)

    # Get table data from schema and table name
    def get_table_data(self, schemaName: str, tableName: str):
        response = self._get("/analytics-svc/api/services/customDBs/{}/{}".format(schemaName, tableName), {})
        return json.loads(response.text)
`,b=`import json
import logging
import os
from pyqe.api.base import _AuthApi
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator
from typing import Optional

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class PAConfig(_AuthApi):
    def __init__(self):
        super().__init__()
        self._default_pa_config: Optional(dict) = None

    async def _get_my_config(self, selectedStudyId):
        params = {
            'action': 'getMyConfig',
            'datasetId': selectedStudyId
        }
        response = await self._get('/analytics-svc/pa/services/analytics.xsjs', params)
        if response.ok:
            return await response.json()

    
    async def _get_study_config_list(self, study):
        params = {
            'action': 'getMyStudyConfigList',
            'datasetId': study
        }
        response = await self._get('/analytics-svc/pa/services/analytics.xsjs', params)
        if response.ok:
            return await response.json()


    async def _get_frontend_config(self, config_id, selectedStudyId, lang = 'eng'):
        params = {
            'action': 'getFrontendConfig',
            'configId': config_id,
            'datasetId': selectedStudyId,
            'lang': lang
        }
        response = await self._get('/analytics-svc/pa/services/analytics.xsjs', params)
        if response.ok:
            return await response.json()
`,C=`import logging
import json
# import requests
import pyodide.http
import os
from typing import Optional, List, Dict
from pyqe.api.base import _AuthApi
from pyqe.api.study import Study
from pyqe.api.pa_config import PAConfig
from pyqe.ql.person import Person
from pyqe.ql.interaction import Interaction, Interactions
from pyqe.ql.filter_card import _ExclusiveFilter, FilterCard
from pyqe.ql.criteria_group import CriteriaGroup
from pyqe.types.enum_types import QueryType, LogicalOperator, MatchCriteria
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator
import asyncio

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class Query(_AuthApi):
    """Query template which can do the following:
        - Select study config for the generated request
        - Add filters and criteria groups
        - Generate the request for the query engine to process

    Args:
        cohort_name: String value defining the cohort name
    """

    def __init__(self, cohort_name: str):
        super().__init__()
        self.cohort_name: str = cohort_name
        self._filters: List[_ExclusiveFilter] = []
        self._interaction_instances: Dict[str, int] = {}
        self._column_config_paths = None
        self._selected_entity_column_config_paths = None
        self.__added_entity_types: List[type] = []
        self.__dynamic_entity_types: List[type] = []
        self._assigned_study_list = []
        self._selectedStudyId = None
        self._study_name = None
        self._study_config = None
        self._study_config_id = None
        self._study_config_version = None
        self._study_config_assigned_name = None

    async def set_study(self, study_id):
        """Set the study to the Query object

        Args:
            study_id: study id\`"""
        if len(self._assigned_study_list) == 0:
            self._assigned_study_list = await Study().get_user_study_list()

        for study in self._assigned_study_list:
            if study['id'] == study_id:
                selected_study = study
                break
        else:
            selected_study = None

        if selected_study == None:
            print("\\n Study not found or assigned to your account \\n")
        else:
            self._study_name = selected_study['studyDetail']['name']
            self._selectedStudyId = selected_study['id']
            os.environ["PYQE_STUDY_ENTITY_VALUE"] = selected_study['id']
            await self.set_study_config()

    async def set_study_config(self):
        """Set the study config to the Query object

        Args:
            config_id: study config Id\`"""
        if self._study_config_id == None:
            self._study_config = await PAConfig()._get_my_config(self._selectedStudyId)

        if len(self._study_config) == 0:
                print("\\n Config not found or assigned to your account \\n")
        else:
            meta = self._study_config[0]['meta']
            self._study_config_id = str(meta['configId'])
            self._study_config_version = str(meta['configVersion'])
            self._study_config_assigned_name = str(meta['configName'])
            await self._build_entities()
        return tuple([{ "config_id" : self._study_config_id}, { "config_version" : self._study_config_version}, { "config_assigned_name" : self._study_config_assigned_name}])

    async def get_study_list(self):
        """Print the list of assigned studies to the user in the format Study Name - Study Id"""

        loop = asyncio.get_event_loop()

        assigned_studies = []
        assigned_studies = await Study().get_user_study_list()

        if len(assigned_studies) == 0:
            print(
                'There are no study assigned to your account\\n')
        else:
            assigned_studies.sort(key=lambda x: x.get('studyDetail').get('name'))
        options = []
        available_studies_for_display = 'Study Name - Study Id'

        self._assigned_study_list = assigned_studies
        for index, study in enumerate(assigned_studies):
            options.append(str(index + 1))
            study_name = study['studyDetail']['name']
            study_id = study['id']
            available_studies_for_display += f'\\n({ index + 1 }) { study_name } - { study_id }'

        print(available_studies_for_display)

    def _clear_selected_study(self):
        self._selectedStudyId = None
        self._study_name = None

    def show_selected_study(self):
        """Print current selected study to console"""
        if self._selectedStudyId != None:
            print(
                f"Currently selected Study Name & ID: { self._study_name }, { self._selectedStudyId }")
        else:
            print(
                'Currently no study selected!')

    def add_criteria_group(self, criteria_group: CriteriaGroup):
        """Add criteria group which will have its filters included in the generated request

        Args:
            criteria_group: CriteriaGroup containing filters

        Raises:
            ValueError: An error occurred if provided criteria_group is invalid
        """
        _has_added = False
        if criteria_group is not None:
            if len(criteria_group._filters) > 0:
                _has_added = True
                self._add_filters_by_criteria_group(criteria_group)
            if len(criteria_group._groups) > 0 and criteria_group._criteria_match == MatchCriteria.ALL:
                _has_added = True
                for exclusive_group in criteria_group._groups:
                    self._add_filters_by_criteria_group(exclusive_group)

        if _has_added is False:
            raise ValueError('Please provide valid CriteriaGroup to add')

    def _add_filters_by_criteria_group(self, criteria_group: CriteriaGroup):
        if criteria_group._criteria_match == MatchCriteria.ANY:
            self.add_filters(criteria_group._filters)
        elif criteria_group._criteria_match == MatchCriteria.ALL:
            for filter in criteria_group._filters:
                self._filters.append(_ExclusiveFilter([filter]))
                self._set_instance_number(filter)
                self.__add_filter_card_class(filter)

    def add_filters(self, filters: List[FilterCard]):
        """Add filters which will be included in the generated request

        Args:
            filters: list of :py:class:\`FilterCard <pyqe.qi.filter_card.FilterCard>\`
        """
        exclusive_filter = _ExclusiveFilter(filters)
        self._filters.append(exclusive_filter)
        for card in exclusive_filter._cards:
            self._set_instance_number(card)
            self.__add_filter_card_class(card)

    def __add_filter_card_class(self, card: FilterCard):
        card_class = card.__class__
        if card_class not in self.__added_entity_types:
            self.__added_entity_types.append(card_class)

    def _set_instance_number(self, filter: FilterCard):
        if isinstance(filter, Interaction):
            interaction_instances = self._interaction_instances
            config_path = filter._config_path
            if config_path in interaction_instances:
                interaction_instances[config_path] += 1
            else:
                interaction_instances[config_path] = 1

            filter._instance_number = interaction_instances[config_path]

    def _clear_study_config(self):
        self._study_config_id = None
        self._study_config_version = None
        self._study_config_assigned_name = None

    def show_current_study_config(self):
        """Print current selected study config to console"""
        if self._study_config_assigned_name != None:
            print(
                f"Currently selected Study Config Name & ID: {self._study_config_assigned_name}, {self._selectedStudyId}")
        else:
            print('Currently no study config is selected!')

    async def _build_entities(self):
        if self._study_config_id is None or self._selectedStudyId is None:
            raise ValueError("Study config ID and selected study ID must be set before building entities.")
        frontend_config = await PAConfig()._get_frontend_config(
           self._study_config_id, self._selectedStudyId, None)
        Person.generate_patient_class(frontend_config)
        Interactions.generate_interaction_type_class(frontend_config)

    async def _configure_columns(self, selected_entity_names: List[str] = []):
        if self._study_config_id == None or self._study_config_assigned_name == None or self._study_config_version == None:
            await self.set_study_config()
        if self._study_config_id is None or self._selectedStudyId is None:
            raise ValueError("Study config ID and selected study ID must be set before configuring columns.")
        fe_config = await PAConfig()._get_frontend_config(
            self._study_config_id, self._selectedStudyId, None)
        if len(fe_config) > 0:
            patient_attributes = fe_config[0]['config']['patient']['attributes'].keys()
        else:
            patient_attributes = {}

        patient_columns = []
        columns = []
        selected_entity_columns = []
        filter_attributes_dict = {'Patient': {}}
        patient_attributes_dict = filter_attributes_dict['Patient']

        for attribute_name in patient_attributes:
            config_path = f'patient.attributes.{attribute_name}'
            patient_columns.append(config_path)
            columns.append(config_path)
            attribute_display_name = fe_config[0]['config']['patient']['attributes'][attribute_name]['name']
            patient_attributes_dict[attribute_display_name] = config_path

        if 'Patient' in selected_entity_names:
            selected_entity_columns = patient_columns

        try:
            fe_config_interactions = fe_config[0]['config']['patient']['interactions']
            interactions = fe_config_interactions.keys()
            for interaction_name in interactions:
                interaction_display_name = fe_config_interactions[interaction_name]['name'].replace(
                    ' ', '')
                if interaction_display_name not in filter_attributes_dict:
                    filter_attributes_dict[interaction_display_name] = {}
                interaction_attributes = fe_config_interactions[interaction_name]['attributes'].keys(
                )

                interaction_columns = []

                for attribute_name in interaction_attributes:
                    config_path = f'patient.interactions.{interaction_name}.attributes.{attribute_name}'
                    columns.append(config_path)
                    interaction_columns.append(config_path)
                    attributes_dict = filter_attributes_dict[interaction_display_name]
                    attribute_display_name = fe_config_interactions[interaction_name]['attributes'][attribute_name]['name']
                    attributes_dict[attribute_display_name] = config_path

                if interaction_display_name in selected_entity_names or interaction_name in selected_entity_names:
                    selected_entity_columns.extend(interaction_columns)

        except (KeyError):
            logger.debug(f'No interaction found in config ID {self._study_config_id}')
        self._column_config_paths = columns
        self._patient_column_config_paths = patient_columns
        self._selected_entity_column_config_paths = selected_entity_columns
        self._filter_attributes_config_paths_dict = filter_attributes_dict
        print(self._selected_entity_column_config_paths)
        return columns

    def _generate_cohort_columns(self, column_config_paths: List[str]):
        seq_count = 0
        cohort_columns = []
        for column_config_path in column_config_paths:
            cohort_columns.append({
                "configPath": column_config_path,
                "order": "",
                "seq": seq_count
            })
            seq_count = seq_count + 1
        return cohort_columns

    async def get_cohort(self, column_config_paths: List[str] = []):
        """Generate the cohort definition request which is used for patients which fits the query criteria

        Args:
            column_config_paths: optional list of column config paths\`"""
        cards = []
        for card in self._filters:
            cards.append(card._req_obj())

        if self._study_config_id == None or self._study_config_assigned_name == None or self._study_config_version == None:
            await self.set_study_config()

        columns = None
        if column_config_paths is not None and len(column_config_paths) > 0:
            columns = self._generate_cohort_columns(column_config_paths)
        if self._column_config_paths is None:
            await self._configure_columns()
        if columns is None:
            columns = self._generate_cohort_columns(self._column_config_paths)

        return {
            'name': self.cohort_name,
            'cohortDefinition': {
                'cards': {
                    'content': cards,
                    'type': QueryType.BOOLEAN_CONTAINER.value,
                    'op': LogicalOperator.AND.value,
                },
                'configData': {
                    'configId': self._study_config_id,
                    'configVersion': self._study_config_version
                },
                'axes': [],
                'guarded': True,
                'offset': 0,
                'columns': columns
            },
            'datasetId': self._selectedStudyId
        }

    async def get_dataframe_cohort(self, column_config_paths=[], selected_entity_name=None):
        """Generate the cohort definition request which is used to download dataframe which fits the query criteria

        Args:
            column_config_paths: optional list of column config paths\`"""

        cards = []
        for card in self._filters:
            cards.append(card._req_obj())

        if self._study_config_id == None or self._study_config_assigned_name == None or self._study_config_version == None:
           await self.set_study_config()

        if selected_entity_name is None:
            if len(self.__added_entity_types) > 1:
                options = []
                choice = None
                entity_choices_for_display = 'Dataframe entity selection:'

                for index, entity_type in enumerate(self.__added_entity_types):
                    options.append(str(index + 1))
                    entity_type_name = entity_type.__name__
                    entity_choices_for_display += f'\\n({index + 1}) {entity_type_name}'

                print(entity_choices_for_display)

                while choice not in options:
                    choice = input(
                        f'Please choose one of the options for the cohort, { options }: ')

                selected_entity_name = self.__added_entity_types[int(choice) - 1].__name__
            else:
                selected_entity_name = self.__added_entity_types[0].__name__

        # print(f'Selected dataframe entity: {selected_entity_name}\\n')

        columns = None
        self._column_config_paths = None
        self._selected_entity_column_config_paths = None
        await self._configure_columns([selected_entity_name])

        columns_in_selected_entity = list(
            filter(lambda x: x in self._selected_entity_column_config_paths, column_config_paths))

        columns = self._generate_cohort_columns(columns_in_selected_entity)
        # columns = self._generate_cohort_columns(column_config_paths)

        if columns is None or len(columns) == 0:
            columns = self._generate_cohort_columns(self._selected_entity_column_config_paths)

        return {
            'cohortDefinition': {
                'cards': {
                    'content': cards,
                    'type': QueryType.BOOLEAN_CONTAINER.value,
                    'op': LogicalOperator.AND.value,
                },
                'configData': {
                    'configId': self._study_config_id,
                    'configVersion': self._study_config_version
                },
                "axes": [],
                "guarded": True,
                "offset": 0,
                'columns': columns
            },
            'datasetId': self._selectedStudyId
        }

    async def get_entities_dataframe_cohort(self, column_config_paths=[]):
        """Generate the cohort definition request for multiple entities which is used to download dataframe which fits the query criteria

        Args:
            column_config_paths: optional list of column config paths\`"""

        entity_cohorts = {}
        columns_not_in_selected_entity = column_config_paths
        for selected_entity in self.__added_entity_types:
            entity_cohorts[selected_entity.__name__] = await self.get_dataframe_cohort(
                column_config_paths=column_config_paths, selected_entity_name=selected_entity.__name__)
            columns_not_in_selected_entity = [
                item for item in columns_not_in_selected_entity if item not in self._selected_entity_column_config_paths]

        self.__dynamic_entity_types = self.get_entities_from_config_paths(
            columns_not_in_selected_entity)

        for dynamic_entity in self.__dynamic_entity_types.keys():
            entity_cohorts[dynamic_entity] = await self.get_dataframe_cohort(
                column_config_paths=column_config_paths, selected_entity_name=dynamic_entity)

        return entity_cohorts

    def get_patient_count_filter(self):
        """Generate the filter request which is used to get patient count which fit the query criteria"""
        cards = []
        for card in self._filters:
            cards.append(card._req_obj())

        return {
            'filter': {
                'configMetadata': {
                    'id': self._study_config_id,
                    'version': self._study_config_version
                },
                'cards': {
                    'content': cards,
                    'type': QueryType.BOOLEAN_CONTAINER.value,
                    'op': LogicalOperator.AND.value,
                }
            },
            'axisSelection': [],
            'metadata': {'version': 3},
            'datasetId': self._selectedStudyId
        }

    def get_entities_from_config_paths(self, column_config_paths=[]):
        dynamic_entities = {}
        for col in column_config_paths:
            if col.startswith("patient.interactions"):
                entity_name = col.split(".")[2]
                if entity_name in dynamic_entities:
                    dynamic_entities[entity_name].append(col)
                else:
                    dynamic_entities[entity_name] = [col]

        return dynamic_entities
`,T=`import logging
from typing import Type
import pandas as pd
import os
from pyqe.api.base import _AuthApi
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator
from pyqe.shared.b64encode_query import _EncodeQueryStringMixin
import base64
logger = logging.getLogger(__name__)
setup_simple_console_log()
import json


@decorator.attach_class_decorator(decorator.log_function, __name__)
class Result(_EncodeQueryStringMixin, _AuthApi):
    """Client to execute request in query engine"""

    def __init__(self):
        super().__init__()
    
    async def get_patient_count(self, filter: dict) -> int:
        """Get patient count which fit the filter request provided

        Args:
            filter: request generated using :py:class:\`Query <pyqe.api.query.Query>\`
        """
        params = {
            'mriquery': self._encode_query_string(filter),
            'datasetId' : os.environ["PYQE_STUDY_ENTITY_VALUE"]
        }
        
        response = await self._get('/analytics-svc/api/services/population/json/patientcount', params)
        res = await response.json()
        patient_count = res['data'][0]['patient.attributes.pcount']
        logger.debug(
            f'Total number of patients based on the filter: {patient_count}')
        return patient_count
    
    async def get_patient_count_by_cohortId(self, cohort: dict, cohortid: int = None) -> int:
        """Get patient count which fit the filter request provided

        Args:
            filter: request generated using :py:class:\`Query <pyqe.api.query.Query>\`
        """
        raw_response =  await self.get_patientCount_api(cohort, cohortId=cohortid)
        patient_count = 0
        if raw_response != None:
            patient_count = json.loads(raw_response)['rowCount']
            
        logger.debug(
            f'Total number of patients based on the filter: {patient_count}')
        return patient_count

    async def _get_patient(self, cohort: dict) -> dict:
        if os.getenv('KERNAL') == 'JUPYTER':
            params = {
                'mriquery': self._encode_query_string(cohort)
            }
        else:
            params = {
                'mriquery': self._encode_query_string(cohort)
            }
        params["datasetId"] = os.environ["PYQE_STUDY_ENTITY_VALUE"]
        print(type(self._encode_query_string(cohort)))
        response = await self._get('/analytics-svc/api/services/patient', params)
        return await response.json()
    #### DEAL WITH STREAMS LATER ##########################
    # def download_stream(self, cohort: dict):
    #     """Download stream from MRI which fit the cohort request provided

    #     Args:
    #         cohort: request generated using :py:class:\`Query <pyqe.api.query.Query>\`
    #     """

    #     params = {
    #         'mriquery': self._encode_query_string(cohort)
    #     }

    #     return self._get_stream('/analytics-svc/api/services/datastream/patient', params)
    ####################

    async def download_dataframe(self, cohort: dict, filename: str = "__temp.csv", cohortid: int = 0, limit: int or bool = False, offset: int = 0):
        """Download dataframe from MRI which fit the cohort request provided

        Args:
            cohort: request generated using :py:class:\`Query <pyqe.api.query.Query>\`
        """
        if limit < 0:
            raise ValueError(f'limit value: {limit} cannot be negative')

        if offset < 0:
            raise ValueError(f'offset value: {offset} cannot be negative')

        cohort['cohortDefinition']['limit'] = limit
        cohort['cohortDefinition']['offset'] = offset

        try:

            if os.path.exists(filename):
                os.remove(filename)
                
            if filename.endswith(".parquet"):
                content = await self.download_raw(cohort, "PARQUET", cohortId=cohortid)

                if not content:
                    return pd.DataFrame(columns=[])

                with open(filename, "wb") as file:  # write binary data
                    file.write(content)

                del content

                response = pd.read_parquet(filename)

                return response
            else:
                text = await self.download_raw(cohort, "CSV", cohortId=cohortid)

                if not text:
                    return pd.DataFrame(columns=[])

                with open(filename, "a") as file:  # Use file to refer to the file object
                    file.write(text)

                del text

                tfr = pd.read_csv(filename, chunksize=1000, iterator=True, engine='python')

                response = pd.concat(tfr, ignore_index=True)

                return response
        except BaseException as error:
            raise error
        finally:
            if os.path.exists(filename):
                os.remove(filename)

    async def download_patient_dataframe(self, entity_cohort: dict, cohortid: int = 0):
        """Download patient dataframe joined with related entities from MRI which fit the cohort request provided

        Args:
            entity_cohort: <k,v> where key is entity name and value is request generated using :py:class:\`Query <pyqe.api.query.Query>\`
        """

        patient_df = await self.download_dataframe(
            entity_cohort["Patient"], "patient.csv", cohortid=cohortid)

        result = None
        for entity_name in entity_cohort.keys():
            if entity_name != "Patient":
                result = patient_df.join(await self.download_dataframe(
                    entity_cohort[entity_name], f"{entity_name}.csv", cohortid=cohortid),
                    lsuffix="_patient",
                    rsuffix=f"_{entity_name}",
                    how="left",
                    on="pid")

        if result is None:
            return patient_df

        return result

    async def download_all_entities_dataframe(self, entity_cohort: dict, cohortId: int = 0):
        """Download all entities into dataframes

        Args:
            entity_cohort: <k,v> where key is entity name and value is request generated using :py:class:\`Query <pyqe.api.query.Query>\`
        """

        result = {}
        for entity_name in entity_cohort.keys():
            result[entity_name] = await self.download_dataframe(
                entity_cohort[entity_name], f"{entity_name}.csv", cohortid=cohortId)

        return result

    async def download_raw(self, cohort: dict, dataFormat: str = "CSV", cohortId: int = 0):
        """Download raw response from MRI which fit the cohort request provided

        Args:
            cohort: request generated using :py:class:\`Query <pyqe.api.query.Query>\`
        """
        if dataFormat.upper() == "PARQUET":
            params = {
                'mriquery': self._encode_query_string(cohort),
                'dataFormat': "PARQUET",
                'cohortId':  str(cohortId),
                'returnOnlyPatientCount': 'False',
                }
        else:
            params = {
                'mriquery': self._encode_query_string(cohort),
                'cohortId': str(cohortId),
                'returnOnlyPatientCount': 'False'
            }

            params["datasetId"] = os.environ["PYQE_STUDY_ENTITY_VALUE"]
        result = await self._get('/analytics-svc/api/services/datastream/patient', params)
        return await result.string()
    
    async def get_patientCount_api(self, cohort: dict, cohortId: int = 0):
        """Get count of patients from MRI which fit the cohort request provided

        Args:
            cohort: request generated using :py:class:\`Query <pyqe.api.query.Query>\`
        """
        params = {
                    'mriquery': self._encode_query_string(cohort),
                    'cohortId':  str(cohortId),
                    'returnOnlyPatientCount': 'True',
                    'datasetId': os.environ["PYQE_STUDY_ENTITY_VALUE"]
                }
        result = await self._get('/analytics-svc/api/services/datastream/patient', params)
        return await result.string()
        
    def get_recontact_info(self, cohort: dict, filename: str):
        """Download encrypted data from MRI which fit the cohort request provided

        Args:
            cohort: request generated using :py:class:\`Query <pyqe.api.query.Query>\`
            filename: name of the file to be created
        """
        params = {
            'mriquery': self._encode_query_string(cohort),
            'datasetId': os.environ["PYQE_STUDY_ENTITY_VALUE"]
        }

        response = self._get('/analytics-svc/api/services/recontact/patient', params)
        with open(f"{filename}.enc", "wb") as g:
            g.write(bytes.fromhex(response.text))

        return f"{filename}.enc created"
`,S=`import json
import logging
import os
from pyqe.api.base import _AuthApi
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator
from typing import Optional

logger = logging.getLogger(__name__)
setup_simple_console_log()

@decorator.attach_class_decorator(decorator.log_function, __name__)
class Study(_AuthApi):
    def __init__(self):
        super().__init__()

    async def get_user_study_list(self):
        params = {
            'role': 'researcher'
            }
        response = await self._get('/system-portal/dataset/list', params)
        if response.ok:
            return await response.json()
        `,N=`"""
About
-------------
\`pyqe.azure\` submodule of \`pyqe\` contains all azure classes
"""
`,E=`import msal


class _MsalCredentials():
    """Provide common functions used for MSAL credentials"""

    def __init__(self, client_id, **kwargs):
        self._client_id = client_id
        self._authority = kwargs.pop('authority', None)
        self._timeout = kwargs.pop('timeout', None)
        self._msal_app = None

    def _get_app(self) -> msal.PublicClientApplication:
        if not self._msal_app:
            self._msal_app = msal.PublicClientApplication(
                client_id=self._client_id,
                authority=self._authority,
                timeout=self._timeout
            )

        return self._msal_app

    def _handle_error(self, result, error_prefix):
        if result.get('error') == 'authorization_pending':
            message = 'Timed out waiting for user to authenticate'
        else:
            error = result.get('error_description') or result.get('error')
            message = f'{error_prefix}: {error}'
        raise RuntimeError(message)
`,w=`import sys
import json
import logging
import os
import getpass
# import requests
import pyodide.http
from typing import Optional
from pyqe.azure.msal_credentials import _MsalCredentials


class _PasswordCredential(_MsalCredentials):
    """Authenticate users through the password grant flow"""

    def __init__(self, client_id, username, **kwargs):
        self._username = username
        super(_PasswordCredential, self).__init__(client_id=client_id, **kwargs)

    def get_token(self, scopes: list, **kwargs):
        app = self._get_app()
        result = None

        # Check the cache to see if this end user has signed in before
        accounts = app.get_accounts(self._username)
        if accounts:
            result = app.acquire_token_silent(scopes, account=accounts[0])

        if not result:
            result = app.acquire_token_by_username_password(
                self._username,
                getpass.getpass(),
                scopes)

        if "id_token" not in result:
            self._handle_error(result, "Authentication failed")

        return result
`,x=`from pyqe.azure.msal_credentials import _MsalCredentials


class _RefreshToken(_MsalCredentials):
    """Retrieve token via refresh token"""

    def __init__(self, client_id, **kwargs):
        super(_RefreshToken, self).__init__(client_id=client_id, **kwargs)

    def get_token(self, refresh_token: str, scopes: list, **kwargs):
        app = self._get_app()
        result = app.acquire_token_by_refresh_token(refresh_token, scopes)

        if 'id_token' not in result:
            error = result.get('error_description') or result.get('error')
            message = f'Refresh token failed: {error}'
            raise RuntimeError(message)

        return result
`,O=`"""
About
-------------
\`pyqe.ql\` submodule of \`pyqe\` contains all query language class definitions

"""
`,D=`import logging
import re
from ..shared import decorator
from ..setup import setup_simple_console_log
from typing import Union
from pyqe.types.enum_types import OriginSelection, TargetSelection

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class AdvanceTimeFilter():
    def __init__(self, targetInteraction: str, originSelection: OriginSelection, targetSelection: TargetSelection = TargetSelection.BEFORE_START,  days: Union[str, int] = 0):

        if not isinstance(targetSelection, TargetSelection):
            raise TypeError('targetSelection must be an instance of TargetSelection Enum')

        if not isinstance(originSelection, OriginSelection):
            raise TypeError('originSelection must be an instance of OriginSelection Enum')

        if not self.validateText(str(days)):
            raise SyntaxError('invalid syntax')

        self._targetInteraction: str = targetInteraction
        self._targetSelection: str = targetSelection.value
        self._originSelection: str = originSelection.value
        self._days: str = str(days)
        self._filters = self.getFilter()
        self._request = self.getRequest()

    def _req_obj(self) -> dict:
        _req_obj: dict = {
            "filters": [self._filters],
            "request": self._request,
            "title": ""
        }
        return _req_obj

    def validateText(self, value):
        rangeRegex = r"^(\\[|\\])\\s?(0|[1-9][0-9]*)\\s?-\\s?(0|[1-9][0-9]*)\\s?(\\[|\\])$"
        operatorRegex = r"^(=|>|<|>=|<=)\\s?(0|[1-9][0-9]*)$"
        numRegex = r"^(0|[1-9][0-9]*)$"

        if value is not None or re.match(rangeRegex, value) or re.match(operatorRegex, value) or re.match(numRegex, value):
            return True
        else:
            return False

    # similar to RegExp.prototype.exec() in javascript
    def regexExec(self, regex, string):
        match = re.search(regex, string)
        if (match):
            return [s for s in match.groups()]

    def getReqObj(self) -> dict:
        return self._req_obj()

    def getFilter(self) -> list:
        if self._originSelection == "overlap":
            return {
                "value": self._targetInteraction,
                "this": "overlap",
                "other": "overlap",
                "after_before": "",
                "operator": ""
            }
        else:
            return {
                "value": self._targetInteraction,
                "this": self._originSelection,
                "other": "startdate" if self._targetSelection == "before_start" or self._targetSelection == "after_start" else "enddate",
                "after_before": "before" if self._targetSelection == "before_start" or self._targetSelection == "before_end" else "after",
                "operator": self._days
            }

    # functions here to determine the filters and request based on input
    def getRequest(self):

        rangeRegex = r"^(\\[|\\])\\s?(0|[1-9][0-9]*)\\s?-\\s?(0|[1-9][0-9]*)\\s?(\\[|\\])$"
        operatorRegex = r"^(=|>|<|>=|<=)\\s?(0|[1-9][0-9]*)$"
        numRegex = r"^(0|[1-9][0-9]*)$"
        timeFilterDataObject = []

        if self._originSelection == "overlap":
            thisContainThat: dict = {
                "value": self._targetInteraction,
                "filter": [
                    {
                        "this": "startdate",
                        "other": "startdate",
                        "and": [
                            {
                                "op": "<",
                                "value": 0
                            }
                        ]
                    },
                    {
                        "this": "enddate",
                        "other": "enddate",
                        "and": [
                            {
                                "op": ">",
                                "value": 0
                            }
                        ]
                    }
                ]
            }
            thatContainThis: dict = {
                "value": self._targetInteraction,
                "filter": [
                    {
                        "this": "startdate",
                        "other": "startdate",
                        "and": [
                            {
                                "op": ">",
                                "value": 0
                            }
                        ]
                    },
                    {
                        "this": "enddate",
                        "other": "enddate",
                        "and": [
                            {
                                "op": "<",
                                "value": 0
                            }
                        ]
                    }
                ]
            }
            thisBeforeThat: dict = {
                "value": self._targetInteraction,
                "filter": [
                    {
                        "this": "enddate",
                        "other": "startdate",
                        "and": [
                            {
                                "op": ">",
                                "value": 0
                            }
                        ]
                    },
                    {
                        "this": "enddate",
                        "other": "enddate",
                        "and": [
                            {
                                "op": "<",
                                "value": 0
                            }
                        ]
                    }
                ]
            }
            thatBeforeThis: dict = {
                "value": self._targetInteraction,
                "filter": [
                    {
                        "this": "startdate",
                        "other": "startdate",
                        "and": [
                            {
                                "op": ">",
                                "value": 0
                            }
                        ]
                    },
                    {
                        "this": "startdate",
                        "other": "enddate",
                        "and": [
                            {
                                "op": "<",
                                "value": 0
                            }
                        ]
                    }
                ]
            }

            timeFilterDataObject.append(
                {"or": [thisContainThat, thatContainThis, thisBeforeThat, thatBeforeThis]})

        else:
            otherObject: dict = {
                "value": self._targetInteraction,
                "filter": []
            }

            filterobj: dict = {
                "this": self._originSelection,
                "other": "startdate" if self._targetSelection == "before_start" or self._targetSelection == "after_start" else "enddate",
                "and": []

            }

            if re.match(rangeRegex, self._days):
                rangeOp = self.regexExec(rangeRegex, self._days)
                operator1 = ">" if rangeOp[0] == "]" else ">="
                value1 = int(rangeOp[1])
                operator2 = "<" if rangeOp[3] == "[" else "<="
                value2 = int(rangeOp[2])

                if self._targetSelection == "after_start" or self._targetSelection == "after_end":
                    operator1 = operator1.replace(">", "<")
                    operator2 = operator2.replace("<", ">")
                    value1 *= -1
                    value2 *= -1

                filterobj['and'].append({
                    "op": operator1,
                    "value": value1
                })

                filterobj['and'].append({
                    "op": operator2,
                    "value": value2
                })

            elif re.match(operatorRegex, self._days):
                opOp = self.regexExec(operatorRegex, self.days)
                operator1 = opOp[0]
                value1 = int(opOp[1])

                if self._targetSelection == "after_start" or self._targetSelection == "after_end":
                    operator1 = operator1.replace(">", "*")
                    operator1 = operator1.replace("<", ">")
                    operator1 = operator1.replace("*", "<")
                    value1 *= -1

                filterobj['and'].append({
                    "op": operator1,
                    "value": value1
                })

            elif re.match(numRegex, self._days):
                value1 = int(self.regexExec(numRegex, self._days)[0])
                operator1 = ">="
                operator2 = "<="

                if self._targetSelection == "after_start" or self._targetSelection == "after_end":
                    value1 *= -1

                filterobj["and"].append({
                    "op": operator1,
                    "value": value1
                })

                filterobj["and"].append({
                    "op": operator2,
                    "value": value1
                })
            else:
                filterobj.update({
                    "and": {
                        "op":  ">" if self._targetSelection == "before_start" or self._targetSelection == "before_end" else "<",
                        "value": 0
                    }
                })

            otherObject['filter'].append(filterobj)
            timeFilterDataObject.append(otherObject)

        return [{"and": timeFilterDataObject}]
`,A=`import logging
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator
from pyqe.ql.date_period import DatePeriod
from pyqe.types.enum_types import QueryType, LogicalOperator, ComparisonOperator
from datetime import datetime
from typing import List

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class Attribute():
    """Attribute of a filter card

        Args:
            config_path: String value of config path defined in CDM

            constraints: list of :py:class:\`Constraint <pyqe.qi.attribute.Constraint>\`
    """

    def __init__(self, config_path: str, constraints: List['Constraint'] = []):
        self._type: str = QueryType.ATTRIBUTE.value
        self._config_path: str = config_path
        self._instance_id: str = config_path
        self._has_added_filter_config_path_and_instance_id: bool = False
        self._constraints: _ExclusiveConstraint = _ExclusiveConstraint(constraints)

    def _req_obj(self) -> dict:
        _req_obj: dict = {
            'type': self._type,
            'configPath': self._config_path,
            'instanceID': self._instance_id,
            'constraints': self._constraints._req_obj()
        }
        return _req_obj


@decorator.attach_class_decorator(decorator.log_function, __name__)
class Expression():
    """Expression which can be added to :py:class:\`Constraint <pyqe.qi.attribute.Constraint>\`

        Args:
            operator: ComparisonOperator for provided value

            value: given value for the expression
    """

    def __init__(self, operator: ComparisonOperator, value: object):
        self._type: str = QueryType.EXPRESSION.value
        self._operator: str = operator.value
        self._value: object = value

    def _req_obj(self) -> dict:
        _req_obj: dict = {
            'type': self._type,
            'operator': self._operator,
            'value': self._value,
        }

        return _req_obj


@decorator.attach_class_decorator(decorator.log_function, __name__)
class _ExclusiveConstraint():
    def __init__(self, constraints: List['Constraint'] = []):
        self._constraints: list = constraints

    def _req_obj(self) -> dict:
        content = []
        for constraint in self._constraints:
            content.append(constraint._req_obj())

        _req_obj: dict = {
            'content': content,
            'type': QueryType.BOOLEAN_CONTAINER.value,
            'op': LogicalOperator.OR.value
        }
        return _req_obj


@decorator.attach_class_decorator(decorator.log_function, __name__)
class Constraint():
    """Constraint given to the filter card"""

    def __init__(self):
        self._content = []

    def add(self, expression: 'Expression'):
        """Add :py:class:\`Expression <pyqe.qi.attribute.Expression>\` to constraint

            Args:
                expression: expression which is defined in the constraint
        """
        self._content.append(expression)
        return self

    def _req_obj(self) -> dict:
        content = []
        for expression in self._content:
            content.append(expression._req_obj())

        _req_obj: dict = {
            'content': content,
            'type': QueryType.BOOLEAN_CONTAINER.value,
            'op': LogicalOperator.AND.value
        }
        return _req_obj


@decorator.attach_class_decorator(decorator.log_function, __name__)
class DateConstraint(Constraint):
    """Constraint which is specifically for date values

        Args:
            date: optional date value

            date_period: optional DatePeriod which specifies a period of 2 dates

        Raises:
            ValueError: An error occurred if neither or both of date and date period parameters are provided
    """

    def __init__(self, date: str = None, date_period: DatePeriod = None):
        if all(v is not None for v in [date, date_period]) or all(v is None for v in [date, date_period]):
            raise ValueError(
                'DateConstraint should only be initialised with one of date or date_period')
        super().__init__()
        if date is not None:
            self._validate_format(date)
            self._add(ComparisonOperator.EQUAL, date)
        elif date_period is not None:
            self._is_after(date_period.start_date)
            self._is_before(date_period.end_date)

    def _validate_format(self, date: str):
        try:
            datetime.strptime(date, '%Y-%m-%d')
            return True
        except ValueError:
            raise ValueError(
                f'date {date} is invalid: it should be a valid date which follows YYYY-MM-DD format')

    def _add(self, comparison_op: ComparisonOperator, date: str):
        self._validate_format(date)
        self._content.append(Expression(comparison_op, date))

    def _is_before(self, date: str):
        self._add(ComparisonOperator.LESS_THAN_EQUAL, date)

    def _is_after(self, date: str):
        self._add(ComparisonOperator.MORE_THAN_EQUAL, date)
`,I=`import os
import json
from pyqe.api.base import _AuthApi
from pyqe.types.enum_types import ConfigPath

FILTER_CARDS = 'filtercards'
MODEL_NAME = 'modelName'
SOURCE = 'source'
ATTRIBUTES = 'attributes'


class Config:
    def __init__(self, path: str, path_type: ConfigPath):
        self._interactions = {}
        if path_type == ConfigPath.URL:
            api = _AuthApi()
            response = api._get(path)
            self._data = json.loads(response.text)
        else:
            with open(os.path.expanduser(path)) as json_file:
                self._data = json.load(json_file)
        if self._data is not None:
            self._patient: dict = self._create_filter_card(self._data[FILTER_CARDS].pop(0))
            for index, card in enumerate(self._data[FILTER_CARDS]):
                self._interactions[card[MODEL_NAME]] = self._create_filter_card(card)
        else:
            raise ValueError('Config setup has failed')

    def _create_filter_card(self, card: dict) -> dict:
        return {
            'configPath': card[SOURCE],
            'name': card[MODEL_NAME],
            ATTRIBUTES: self._create_attributes(card[ATTRIBUTES])
        }

    def _create_attributes(self, config_attributes: list) -> dict:
        attributes = {}
        for config_attribute in config_attributes:
            attribute = ATTRIBUTES + config_attribute[SOURCE].split('.attributes', 1)[1]
            attributes[config_attribute[MODEL_NAME]] = attribute
        return attributes

    def find_interaction(self, name: str) -> dict:
        return self._interactions[name]`,q=`import logging
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator
from pyqe.ql.filter_card import FilterCard
from pyqe.types.enum_types import MatchCriteria
from typing import List

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class CriteriaGroup:
    """A group of criteria to determine if any or all of provided criteria should be matched

        Args:
            criteria_match: MatchCriteria value to determine if any or all of provided filter cards should be matched

            filters: list of :py:class:\`FilterCard <pyqe.qi.filter_card.FilterCard>\`

        Raises:
            ValueError: An error occurred if provided criteria_match is None
    """

    def __init__(self, criteria_match: MatchCriteria, filters: List[FilterCard] = []):
        if criteria_match is None:
            raise ValueError('Please provide valid MatchCriteria value for CriteriaGroup')

        self._criteria_match: MatchCriteria = criteria_match
        self._filters: List[FilterCard] = filters
        self._groups: List['CriteriaGroup'] = []

    def add_exclusive_group(self, criteria_group: 'CriteriaGroup'):
        """Add exclusive CriteriaGroup which has to be matched.

        Args:
            criteria_group: CriteriaGroup variable which is exclusive

        An example of its usage will be:

        .. code-block:: python

            # criteria which requires either of female or male gender
            exclusive_group = CriteriaGroup(MatchCriteria.ANY, [female_gender, male_gender])
            # criteria which requires both diabetes & heart conditions
            criteria_group = CriteriaGroup(MatchCriteria.ALL, [diabetes_condition, heart_condition])
            # criteria which requires both diabetes & heart conditions and either of female or male gender
            criteria_group.add_exclusive_group(criteria_group)
        """
        self._groups.append(criteria_group)
`,P=`import logging
import datetime
import enum
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator
from pyqe.types.enum_types import ComparisonOperator, Format
from typing import List

logger = logging.getLogger(__name__)
setup_simple_console_log()

DATE_FORMAT = Format.DATE.value


@decorator.attach_class_decorator(decorator.log_function, __name__)
class DatePeriod():
    """Data structure representing a period of 2 dates

        Args:
            date: value to determine the start date or end date of period based on provided date_period_type & is_inclusive parameters

            number_of_days: number of days from the date

            date_period_type: value to determine if date is before or after the number_of_days in the period

            is_inclusive: optional boolean value to determine if date parameter is inclusive in the period.

        Raises:
            ValueError: An error occurred if any of the provided parameters is invalid
    """

    def __init__(self, date: str, number_of_days: int, date_period_type: 'DatePeriod.Type', is_inclusive: bool = True):
        self._validate_inputs(date, number_of_days, date_period_type, is_inclusive)
        if date_period_type is DatePeriod.Type.BEFORE:
            if is_inclusive:
                self._end_date = date
            else:
                self._end_date = self._compute_date(date, -1)
            self._start_date = self._compute_date(self._end_date, -number_of_days + 1)
        if date_period_type is DatePeriod.Type.AFTER:
            if is_inclusive:
                self._start_date = date
            else:
                self._start_date = self._compute_date(date, 1)
            self._end_date = self._compute_date(self._start_date, number_of_days - 1)

    @property
    def start_date(self) -> str:
        return self._start_date

    @property
    def end_date(self) -> str:
        return self._end_date

    def _validate_inputs(self, date: str, number_of_days: int, date_period_type: 'DatePeriod.Type', is_inclusive: bool):
        if date is None:
            raise ValueError(f'date {date} is invalid')
        try:
            datetime.datetime.strptime(date, DATE_FORMAT)
        except ValueError:
            raise ValueError(
                f'date {date} is invalid: it should be a valid date which follows YYYY-MM-DD format')
        if number_of_days is None or number_of_days <= 0:
            raise ValueError(f'number_of_days should be positive')
        elif date_period_type is None or not isinstance(date_period_type, DatePeriod.Type):
            raise ValueError(f'date_period_type should be either BEFORE or AFTER')
        elif is_inclusive is None or not isinstance(is_inclusive, bool):
            raise ValueError(f'is_inclusive should be a boolean value')
        return True

    def _compute_date(self, date: str, number_of_days: int):
        computed_date = datetime.datetime.strptime(
            date, DATE_FORMAT) + datetime.timedelta(days=number_of_days)
        return computed_date.strftime(DATE_FORMAT)

    class Type(enum.Enum):
        BEFORE = 'BEFORE'
        AFTER = 'AFTER'


class CurrentDatePeriod(DatePeriod):
    """Data structure representing a period of 2 dates with one based on today date

        Args:
            number_of_days: number of days from today date

            date_period_type: value to determine if today date is before or after the number_of_days in the period

            is_inclusive: optional boolean value to determine if today date is inclusive in the period

        Raises:
            ValueError: An error occurred if any of the provided parameters is invalid
    """

    def __init__(self, number_of_days: int, date_period_type: 'DatePeriod.Type', is_inclusive: bool = True):
        today = self._get_today()
        super().__init__(today, number_of_days, date_period_type, is_inclusive)

    def _get_today(self):
        return datetime.date.today().strftime(DATE_FORMAT)
`,k=`import logging
from pyqe.ql.advanced_time_filter import AdvanceTimeFilter
from pyqe.setup import setup_simple_console_log
from pyqe.shared import decorator
from pyqe.api.concept_query import ConceptSet
from pyqe.types.enum_types import QueryType, FilterInfo, LogicalOperator, Domain, CardType
from pyqe.ql.attribute import *
from typing import List, Dict

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class _ExclusiveFilter():
    def __init__(self, cards: List['FilterCard'] = []):
        self._cards: list = cards

    def _req_obj(self) -> dict:
        card_content = []
        for card in self._cards:
            card_content.append(card._req_obj())

        _req_obj: dict = {
            'content': card_content,
            'type': QueryType.BOOLEAN_CONTAINER.value,
            'op': LogicalOperator.OR.value
        }
        return _req_obj


@decorator.attach_class_decorator(decorator.log_function, __name__)
class FilterCard():
    def __init__(self, name: str, config_path: str, card_type: CardType = CardType.INCLUDED):
        self._type: str = QueryType.FILTER_CARD.value
        self._inactive: bool = False
        self._name: str = name
        self._config_path: str = config_path
        self._attributes: List[Attribute] = []
        self._concept_sets: Dict[Domain, (List[str], List[str])] = {}
        self.card_type: CardType = card_type
        self._advance_time_filter: List[AdvanceTimeFilter] = []

    def add_attribute(self, attribute: Attribute):
        """Add attribute in filter"""
        self._attributes.append(attribute)

    def add_concept_set(self, concept_set: ConceptSet):
        """Add concept set in filter"""
        if concept_set.domain is None:
            raise ValueError('Domain is missing in concept set')
        self._concept_sets[concept_set.domain] = (concept_set.concept_codes,
                                                  concept_set.excluded_concept_codes)

    def add_patient_id(self, constraints: List[Constraint] = []):
        self.add_attribute(PatientId(constraints))
        return self

    def _create_attribute_content(self) -> list:
        attribute_content = []
        for attribute in self._attributes:
            if attribute._has_added_filter_config_path_and_instance_id is False:
                attribute._config_path = '.'.join([self._config_path, attribute._config_path])
                attribute._instance_id = '.'.join([self._instance_id, attribute._instance_id])
                attribute._has_added_filter_config_path_and_instance_id = True

            attribute_content.append(attribute._req_obj())

        for domain in self._concept_sets.keys():
            attribute_content.append(self._create_concept_code_attribute(domain)._req_obj())

        return attribute_content

    def _create_concept_code_attribute(self, domain):
        constraints = []
        values = self._concept_sets[domain][0]
        excluded_values = self._concept_sets[domain][1]
        if len(values) > 0:
            for constraint_value in values:
                constraints.append(self._create_concept_code_constraint(
                    constraint_value, excluded_values))
        elif len(excluded_values) > 0:
            constraints.append(self._create_excluded_constraint(excluded_values))

        attribute = Attribute(
            f'attributes.{domain.value}conceptcode', constraints)
        attribute._config_path = '.'.join(
            [self._config_path, attribute._config_path])
        attribute._instance_id = '.'.join(
            [self._instance_id, attribute._instance_id])
        return attribute

    def _create_concept_code_constraint(self, constraint_value, excluded_values: List[str]):
        concept_code_constraint = self._create_excluded_constraint(
            excluded_values).add(Expression(ComparisonOperator.EQUAL, constraint_value))
        return concept_code_constraint

    def _create_excluded_constraint(self, excluded_values: List[str]):
        excluded_constraint = Constraint()
        for excluded_constraint_value in excluded_values:
            excluded_constraint.add(Expression(
                ComparisonOperator.NOT_EQUAL, excluded_constraint_value))
        return excluded_constraint

    def _create_advance_time_filter(self) -> dict:
        timeFilter = None
        for index, advTimeFilter in enumerate(self._advance_time_filter):
            if index == 0:
                timeFilter = advTimeFilter.getReqObj()
            else:
                timeFilter['filters'].append(advTimeFilter.getFilter())
                timeFilter['request'][0]['and'].append(advTimeFilter.getRequest()[0]['and'][0])

        return timeFilter

    def _req_obj(self) -> dict:
        _req_obj: dict = {
            'type': self._type,
            'inactive': self._inactive,
            'name': self._name,
            'configPath': self._config_path,
            'instanceNumber': self._instance_number,
            'instanceID': self._instance_id,
            'attributes': {
                'content': self._create_attribute_content(),
                'type': QueryType.BOOLEAN_CONTAINER.value,
                'op': LogicalOperator.AND.value
            },
            'advanceTimeFilter': self._create_advance_time_filter()
        }

        if self.card_type == CardType.INCLUDED:
            return _req_obj
        elif self.card_type == CardType.EXCLUDED:
            return {
                'content': [_req_obj],
                'type': QueryType.BOOLEAN_CONTAINER.value,
                'op': LogicalOperator.NOT.value
            }
        else:
            raise ValueError(f'Invalid filter card type: {self.card_type}')
`,L=`import logging
from ..shared import decorator
from ..setup import setup_simple_console_log
from .filter_card import FilterCard
from .attribute import Attribute, Constraint
from .advanced_time_filter import AdvanceTimeFilter
from ..types.enum_types import CardType
from typing import List

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class Interaction(FilterCard):
    def __init__(self, name: str, config_path: str, card_type: CardType = CardType.INCLUDED):
        self._instance_number = 1
        super().__init__(name, config_path, card_type)

    def add_advance_time_filter(self, advanceTimeFilter: AdvanceTimeFilter):
        self._advance_time_filter.append(advanceTimeFilter)

    def _req_obj(self) -> dict:
        self._instance_id = self._config_path + '.' + str(self._instance_number)
        return super()._req_obj()

    def get_instance_id(self) -> str:
        return self._config_path + '.' + str(self._instance_number)


class Interactions():
    _frontend_config = None

    @staticmethod
    def generate_interaction_type_class(frontend_config: object):
        def delete_old_interactions():
            interactions = Interactions._get_interactions(Interactions._frontend_config)

            for key in interactions.keys():
                interaction_name = interactions[key]['name']
                name_for_class = Interactions._get_interaction_class_name(interaction_name)
                delattr(Interactions, name_for_class)

        if Interactions._frontend_config is not None:
            delete_old_interactions()

        Interactions._frontend_config = frontend_config
        Interactions._build_interaction_type_class(frontend_config)

    @staticmethod
    def _build_interaction_type_class(frontend_config: dict):
        def build_attribute_function_for_interation_type_class(class_name: str, interaction_attributes: List):
            if (class_name == 'PayerPlanPeriod'):
                function_code = """def func(self, concept_set):
                                        error_message = 'Payer Plan Period does not have concept available'
                                        logger.error(error_message)
                                        raise ValueError(error_message)
                                """
                exec(function_code, globals())
                setattr(globals()[class_name], 'add_concept_set', func)

            for key in interaction_attributes.keys():
                function_code = """def func(self, constraints: List[Constraint] = []): 
                                        self.add_attribute(Attribute('attributes.{0}', constraints)) 
                                        return self
                                """.format(key)
                exec(function_code, globals())

                attribute_name = interaction_attributes[key]['name']
                function_name = 'add_{0}'.format(attribute_name.lower().replace(' ', '_').replace(
                    ',', '_').replace('/', '_').replace('(', '').replace(')', ''))
                setattr(globals()[class_name], function_name, func)

        interactions = Interactions._get_interactions(frontend_config)

        for key in interactions.keys():
            interaction_name = interactions[key]['name']
            name_for_class = Interactions._get_interaction_class_name(interaction_name)
            class_code = """class {0}(Interaction):
                                def __init__(self, name: str, card_type: CardType = CardType.INCLUDED):
                                    super().__init__(name, 'patient.interactions.{1}', card_type)
                         """.format(name_for_class, key)

            exec(class_code, globals())
            build_attribute_function_for_interation_type_class(
                name_for_class, interactions[key]['attributes'])
            setattr(Interactions, name_for_class, globals()[name_for_class])

    @staticmethod
    def _get_interactions(frontend_config: dict):
        try:
            if len(frontend_config) > 0:
                return frontend_config[0]['config']['patient']['interactions']
            else:
                return {}
        except (KeyError):
            logger.debug(f'No interaction found in frontend config')
            return {}

    @staticmethod
    def _get_interaction_class_name(value: str):
        return value.replace(' ', '').replace(',', '').replace('/', '').replace('(', '').replace(')', '')
`,F=`import logging
from ..shared import decorator
from ..setup import setup_simple_console_log
from ..types.enum_types import FilterInfo, CardType
from .filter_card import FilterCard
from .attribute import Attribute, Constraint
from typing import List

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class Person():
    # It is required to generate the Patient class during runtime to allow intellisense to work
    # For Patient, there is no need to reset the state of the class when we rerun a new study config
    # because the new Patient class will overwrite the previous since they share a common attribute name
    @staticmethod
    def generate_patient_class(frontend_config: dict):
        def build_attribute_function(frontend_config: dict):
            if len(frontend_config) > 0:
                patient_attribute = frontend_config[0]['config']['patient']['attributes']

            for key in patient_attribute.keys():
                function_code = """def func(self, constraints: List[Constraint] = []): 
                                        self.add_attribute(Attribute('attributes.{0}', constraints)) 
                                        return self
                                """.format(key)

                exec(function_code, globals())

                attribute_name = patient_attribute[key]['name']
                function_name = 'add_{0}'.format(attribute_name.lower().replace(' ', '_').replace(
                    ',', '_').replace('/', '_').replace('(', '').replace(')', ''))
                setattr(Patient, function_name, func)

        class_code = """class Patient(FilterCard):
                            def __init__(self, card_type: CardType = CardType.INCLUDED):
                                patient = FilterInfo.PATIENT.value
                                self._instance_id = patient
                                self._instance_number = 0
                                super().__init__(FilterInfo.BASIC_DATA.value, patient, card_type)
                    """

        exec(class_code, globals())
        build_attribute_function(frontend_config)

        setattr(Person, 'Patient', globals()['Patient'])
`,M=`feature-flags:
  azure-identity: true
`,R=`import os
import logging
import logging.config
import yaml
import pkgutil


def setup_log_from_file(log_config_file=None, default_level=logging.INFO):
    """Setup logging configuration from yaml file"""

    logger = logging.getLogger("pyqe")
    logger.setLevel(default_level)
    logger.info('Setting up logging')

    path = log_config_file if log_config_file else os.getenv('PYQE_LOG_CONFIG_FILE', None)
    if not path:
        path = 'logging.yaml'

    exists = os.path.exists(path)
    if exists:
        logger.info('Reading logging configs from file')
        with open(path, 'rt') as f:
            data = f.read()
    else:
        logger.info('Reading logging configs from package')
        data = pkgutil.get_data('pyqe', 'logging.yaml')

    if data:
        logger.info('Configuring logging')
        config = yaml.safe_load(data)
        logging.config.dictConfig(config)
    else:
        logger.warning(f'File {path} is not found')


def setup_simple_console_log(
    default_level=logging.INFO,
    default_format='%(asctime)s - %(levelname)s - %(message)s',
    default_datefmt='%d-%b-%Y %H:%M:%S'
):
    """Setup a simple console logging"""
    logging.basicConfig(format=default_format, datefmt=default_datefmt)
    logger = logging.getLogger("pyqe")
    logger.setLevel(default_level)
`,j=`"""
About
-------------
\`pyqe.shared\` submodule of \`pyqe\` contains shared functions

"""
`,z=`import json
import base64
import zlib
from urllib.parse import unquote, quote_from_bytes
import logging
from pyqe.shared import decorator
from pyqe.setup import setup_simple_console_log

logger = logging.getLogger(__name__)
setup_simple_console_log()


@decorator.attach_class_decorator(decorator.log_function, __name__)
class _EncodeQueryStringMixin():
    """
    shared functions for base64 encoding query string
    """

    def __init__(self):
        super().__init__()

    def _encode_query_string(self, data):
        print(type(data))
        return quote_from_bytes(base64.b64encode(self._pako_deflate(json.dumps(data))))
        
    def _pako_deflate(self, data):
        compress = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, 15,
                                    memLevel=8, strategy=zlib.Z_DEFAULT_STRATEGY)
        compressed_data = compress.compress(bytes(unquote(data), 'iso-8859-1'))
        compressed_data += compress.flush()
        return compressed_data
`,U=`import functools
import inspect
import logging
import uuid
import re
from time import time
from pyodide.http import FetchResponse
from typing import Any, List


SECRET_FUNCTIONS: List[Any] = []


def attach_class_decorator(decorator, *args, **kwargs):
    """Attach decorator to all methods within the given class"""
    def theclass(cls):
        for name, func in inspect.getmembers(cls, inspect.isfunction):
            setattr(cls, name, decorator(func, *args, **kwargs))
        return cls
    return theclass


def attach_function_decorator(decorator, *args, **kwargs):
    """Attach decorator to function"""
    def thefunction(func):
        return decorator(func, *args, **kwargs)
    return thefunction


def log_function(func, logger_name=None):
    """Perform logging when function is called and return"""
    logger = logging.getLogger(logger_name or __name__)

    @functools.wraps(func)
    def decorated(*args, **kwargs):
        # only for debug, otherwise skip below
        if not logger.isEnabledFor(logging.DEBUG):
            return func(*args, **kwargs)

        log_id = uuid.uuid4()

        is_secret: bool = _require_masking(func.__name__)
        if is_secret:
            logger.debug(f'[{log_id}] {(func.__name__)} <<REDACTED>>')
            logger.setLevel(logging.INFO)

        logger.debug(f'[{log_id}] {(func.__name__)} {args} - {kwargs}')

        start = time()
        result = func(*args, **kwargs)
        end = time()

        return_result = ''
        if result is not None:
            return_result = result
            if isinstance(result, FetchResponse) and result.text:
                return_result = result.text
            return_result = f' => {return_result}'

        elapsed = '{:.5f} seconds'.format(end - start)

        if is_secret:
            logger.setLevel(logging.DEBUG)

        logger.debug(f'[{log_id}] {(func.__name__)}{return_result} ({elapsed})')

        return result

    return decorated


def _require_masking(func_name: str) -> bool:
    for fn in SECRET_FUNCTIONS:
        if type(fn) is str:
            if fn == func_name:
                return True
        elif type(fn) is re.Pattern:
            if fn.match(func_name) is not None:
                return True

    return False
`,G=`import pkgutil
import yaml
from typing import Any


data: Any = pkgutil.get_data('pyqe', 'settings.yaml')
settings: Any = yaml.safe_load(data)


def is_feature(feature_name: str) -> bool:
    global settings
    setting_name = 'feature-flags'
    if settings and setting_name in settings and feature_name in settings[setting_name]:
        return settings[setting_name][feature_name]

    return False
`,V=`"""
About
-------------
\`pyqe.types\` submodule of \`pyqe\` contains enum definitions

"""
`,Q=`import enum


class ConfigPath(enum.Enum):
    LOCAL_FILE = 'LocalFile'
    URL = 'Url'


class QueryType(enum.Enum):
    FILTER_CARD = 'FilterCard'
    BOOLEAN_CONTAINER = 'BooleanContainer'
    ATTRIBUTE = 'Attribute'
    EXPRESSION = 'Expression'


class FilterInfo(enum.Enum):
    PATIENT = 'patient'
    BASIC_DATA = 'Basic Data'


class LogicalOperator(enum.Enum):
    AND = 'AND'
    OR = 'OR'
    NOT = 'NOT'


class CardType(enum.Enum):
    INCLUDED = 'INCLUDED'
    EXCLUDED = 'EXCLUDED'


class ComparisonOperator(enum.Enum):
    EQUAL = '='
    NOT_EQUAL = '<>'
    MORE_THAN_EQUAL = '>='
    LESS_THAN_EQUAL = '<='
    MORE_THAN = '>'
    LESS_THAN = '<'


class MatchCriteria(enum.Enum):
    ALL = 'ALL'
    ANY = 'ANY'


class Domain(enum.Enum):
    ETHNICITY = 'ethnicity'
    GENDER = 'gender'
    RACE = 'race'
    VISIT = 'visit'
    VISIT_TYPE = 'visittype'
    SPECIMEN = 'specimen'
    UNIT = 'unit'
    ANATOMIC_SITE = 'anatomicsite'
    DISEASE_STATUS = 'diseasestatus'
    SPECIMEN_TYPE = 'specimentype'
    PROCEDURE = 'proc'
    PROCEDURE_TYPE = 'proctype'
    MODIFIER = 'modifier'
    OBSERVATION_PERIOD_TYPE = 'periodtype'
    OBSERVATION = 'obs'
    OBSERVATION_TYPE = 'obstype'
    VALUE_AS = 'valueas'
    QUALIFIER = 'qualifier'
    MEASUREMENT = 'measurement'
    MEASUREMENT_TYPE = 'measurementtype'
    DRUG = 'drug'
    DRUG_TYPE = 'drugtype'
    ROUTE = 'route'
    DEVICE = 'device'
    DEVICE_TYPE = 'devicetype'
    DEATH_TYPE = 'deathtype'
    CONDITION = 'cond'
    CONDITION_TYPE = 'conditiontype'
    CONDITION_SOURCE = 'conditionsource'
    CONDITION_STATUS = 'conditionstatus'


class Format(enum.Enum):
    DATE = '%Y-%m-%d'
    DATETIME = '%Y-%m-%d %H:%M:%S'


class OriginSelection(enum.Enum):
    STARTED = 'startdate'
    ENDED = 'enddate'
    OVERLAP = 'overlap'


class TargetSelection(enum.Enum):
    BEFORE_START = 'before_start'
    AFTER_START = 'after_start'
    BEFORE_END = 'before_end'
    AFTER_END = 'after_end'
`,B=`from dataclasses import dataclass
from typing import List


@dataclass
class ConceptSetConcept:
    id: int
    useMapped: bool
    useDescendants: bool


@dataclass
class ConceptSet:
    id: int
    name: str
    shared: bool
    concepts: List[ConceptSetConcept]
    userName: str
    createdBy: str
    modifiedBy: str
    createdDate: str
    modifiedDate: str
`,Y=`"""
About
-------------
\`pyqe.utils\` submodule of \`pyqe\` contains utility functions and constants

"""
`,W=`"""
Strategus Spec Builder for Pyodide

Standalone Strategus Analysis Specification Builder for Pyodide (browser-based Python).
This module provides functions to create Strategus analysis specifications
without requiring Java dependencies or R packages.

Usage:
    # In Pyodide (browser)
    from strategus_spec_builder import *

    # Or load from URL
    import pyodide_http
    pyodide_http.patch_all()
    exec(requests.get("https://raw.githubusercontent.com/OHDSI/Strategus/main/inst/pyodide/strategus_spec_builder.py").text)

Dependencies:
    - None (pure Python, works in Pyodide)

Based on Strategus v1.4.1

HADES Package Version Tracking (for maintenance):
    - CohortMethod 5.4.0
    - CohortDiagnostics 3.3.0
    - FeatureExtraction 3.7.0
    - Characterization 2.0.0
    - Cyclops 3.5.0
    - SelfControlledCaseSeries (latest)
    - PatientLevelPrediction (latest)
    - EvidenceSynthesis (latest)
    - CohortIncidence (latest)
    - CohortSurvival (darwin-eu)

Note: Default settings are inlined from the HADES packages listed above.
If HADES package defaults change, this file may need updates.
"""

from __future__ import annotations
import json
import math
from dataclasses import dataclass, field, asdict
from typing import Any, Optional, Union


# =============================================================================
# Specification Classes
# =============================================================================

@dataclass
class AnalysisSpecifications:
    """Container for Strategus analysis specifications."""
    shared_resources: list = field(default_factory=list)
    module_specifications: list = field(default_factory=list)
    _class: str = field(default="AnalysisSpecifications", repr=False)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "sharedResources": self.shared_resources,
            "moduleSpecifications": self.module_specifications
        }

    def to_json(self, pretty: bool = True) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=2 if pretty else None)


@dataclass
class SharedResources:
    """Base class for shared resources."""
    _class: tuple = field(default=("SharedResources",), repr=False)

    def to_dict(self) -> dict:
        """Convert to dictionary, excluding private fields."""
        result = {}
        for k, v in self.__dict__.items():
            if not k.startswith("_"):
                result[k] = v
        return result


@dataclass
class ModuleSpecifications:
    """Base class for module specifications."""
    module: str
    settings: dict
    _class: tuple = field(default=("ModuleSpecifications",), repr=False)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "module": self.module,
            "settings": self.settings
        }


# =============================================================================
# Internal Helper Functions
# =============================================================================

def _is_cohort_definition_set(df: list[dict]) -> bool:
    """Validate cohort definition set structure."""
    if not isinstance(df, list) or len(df) == 0:
        return False
    required_cols = {"cohortId", "cohortName", "sql", "json"}
    return all(required_cols.issubset(row.keys()) for row in df)


def _listafy(df: list[dict]) -> list[dict]:
    """Convert cohort definition set to list format for JSON."""
    result = []
    for row in df:
        cohort_data = {
            "cohortId": row["cohortId"],
            "cohortName": row["cohortName"],
            "cohortDefinition": row["json"]
        }
        result.append(cohort_data)
    return result


# =============================================================================
# Inlined Default Settings (from HADES packages)
# =============================================================================

def _create_default_cm_diagnostic_thresholds() -> dict:
    """CohortMethod diagnostic thresholds (from CohortMethod::createCmDiagnosticThresholds)."""
    return {
        "mdrrThreshold": 10,
        "easeThreshold": 0.25,
        "sdmThreshold": 0.1,
        "equipoiseThreshold": 0.2,
        "generalizabilitySdmThreshold": 1,
        "_class": "CmDiagnosticThresholds"
    }


def _create_default_es_diagnostic_thresholds() -> dict:
    """Evidence Synthesis diagnostic thresholds."""
    return {
        "mdrrThreshold": 10,
        "easeThreshold": 0.25,
        "i2Threshold": 0.4,
        "tauThreshold": math.log(2),  # ~0.693
        "_class": "EsDiagnosticThresholds"
    }


def _get_default_characterization_covariate_settings() -> dict:
    """Characterization covariate settings (from FeatureExtraction)."""
    return {
        "temporal": False,
        "temporalSequence": False,
        # Demographics - all enabled
        "useDemographicsGender": True,
        "useDemographicsAge": True,
        "useDemographicsAgeGroup": True,
        "useDemographicsRace": True,
        "useDemographicsEthnicity": True,
        "useDemographicsIndexYear": True,
        "useDemographicsIndexMonth": True,
        "useDemographicsTimeInCohort": True,
        "useDemographicsPriorObservationTime": True,
        "useDemographicsPostObservationTime": True,
        # Long term covariates
        "useConditionGroupEraLongTerm": True,
        "useDrugGroupEraOverlapping": True,
        "useDrugGroupEraLongTerm": True,
        "useProcedureOccurrenceLongTerm": True,
        "useMeasurementLongTerm": True,
        "useObservationLongTerm": True,
        "useDeviceExposureLongTerm": True,
        "useVisitConceptCountLongTerm": True,
        # Short term covariates
        "useConditionGroupEraShortTerm": True,
        "useDrugGroupEraShortTerm": True,
        "useProcedureOccurrenceShortTerm": True,
        "useMeasurementShortTerm": True,
        "useObservationShortTerm": True,
        "useDeviceExposureShortTerm": True,
        "useVisitConceptCountShortTerm": True,
        # Time windows
        "endDays": 0,
        "longTermStartDays": -365,
        "shortTermStartDays": -30,
        # Concept filtering
        "includedCovariateConceptIds": [],
        "excludedCovariateConceptIds": [],
        "includedCovariateIds": [],
        "addDescendantsToInclude": False,
        "addDescendantsToExclude": False,
        "_class": "covariateSettings",
        "_fun": "getDbCovariateData"
    }


def _get_default_case_covariate_settings() -> dict:
    """Characterization case (during) covariate settings."""
    return {
        "useConditionGroupEraDuring": True,
        "useDrugGroupEraDuring": True,
        "useProcedureOccurrenceDuring": True,
        "useDeviceExposureDuring": True,
        "useMeasurementDuring": True,
        "useObservationDuring": True,
        "useVisitConceptCountDuring": True,
        "_class": "covariateSettings",
        "_fun": "Characterization::getDuringCovariateData"
    }


def _get_default_temporal_covariate_settings() -> dict:
    """CohortDiagnostics temporal covariate settings."""
    return {
        "temporal": True,
        "temporalSequence": False,
        # Condition covariates
        "useConditionEraGroupStart": True,
        "useConditionEraGroupOverlap": True,
        # Drug covariates
        "useDrugEraGroupStart": True,
        "useDrugEraGroupOverlap": True,
        # Visit covariates
        "useVisitConceptCountStart": True,
        "useVisitConceptCountOverlap": True,
        # Time windows (mandatory for CohortDiagnostics)
        "temporalStartDays": [-365, -30, -365, -30, 0, 1, 31, -9999],
        "temporalEndDays": [0, 0, -31, -1, 0, 30, 365, 9999],
        # Concept filtering
        "includedCovariateConceptIds": [],
        "excludedCovariateConceptIds": [],
        "includedCovariateIds": [],
        "addDescendantsToInclude": False,
        "addDescendantsToExclude": False,
        "_class": "covariateSettings",
        "_fun": "getDbCovariateData"
    }


# =============================================================================
# Cyclops Helper Functions
# =============================================================================

def create_prior(prior_type: str = "laplace",
                 variance: float = 1,
                 exclude: list = None,
                 graph: Any = None,
                 neighborhood: Any = None,
                 use_cross_validation: bool = True,
                 force_intercept: bool = False) -> dict:
    """Create a Cyclops prior specification."""
    return {
        "priorType": prior_type,
        "variance": variance,
        "exclude": exclude if exclude is not None else [],
        "graph": graph,
        "neighborhood": neighborhood,
        "useCrossValidation": use_cross_validation,
        "forceIntercept": force_intercept,
        "_class": "cyclopsPrior"
    }


def create_control(max_iterations: int = 1000,
                   tolerance: float = 1e-6,
                   convergence_type: str = "gradient",
                   auto_search: bool = True,
                   fold: int = 10,
                   cv_repetitions: int = 1,
                   starting_variance: float = 0.01,
                   lower_limit: float = 0.01,
                   upper_limit: float = 20,
                   seed: Optional[int] = None,
                   reset_coefficients: bool = False,
                   noise_level: str = "silent",
                   threads: int = 1,
                   cv_type: str = "auto",
                   selector_type: str = "byPid") -> dict:
    """Create a Cyclops control specification."""
    return {
        "maxIterations": max_iterations,
        "tolerance": tolerance,
        "convergenceType": convergence_type,
        "autoSearch": auto_search,
        "fold": fold,
        "cvRepetitions": cv_repetitions,
        "startingVariance": starting_variance,
        "lowerLimit": lower_limit,
        "upperLimit": upper_limit,
        "seed": seed,
        "resetCoefficients": reset_coefficients,
        "noiseLevel": noise_level,
        "threads": threads,
        "cvType": cv_type,
        "selectorType": selector_type,
        "_class": "cyclopsControl"
    }


# =============================================================================
# FeatureExtraction Builder Functions
# =============================================================================

def create_covariate_settings(**kwargs) -> dict:
    """Create covariate settings for FeatureExtraction.

    Accepts all use* boolean flags (default False) plus:
    - long_term_start_days (-365), medium_term_start_days (-180),
      short_term_start_days (-30), end_days (0)
    - included/excluded covariate concept/covariate IDs
    """
    _USE_FLAGS = [
        "useDemographicsGender", "useDemographicsAge", "useDemographicsAgeGroup",
        "useDemographicsRace", "useDemographicsEthnicity", "useDemographicsIndexYear",
        "useDemographicsIndexMonth", "useDemographicsPriorObservationTime",
        "useDemographicsPostObservationTime", "useDemographicsTimeInCohort",
        "useDemographicsIndexYearMonth", "useCareSiteId",
        "useConditionOccurrenceAnyTimePrior", "useConditionOccurrenceLongTerm",
        "useConditionOccurrenceMediumTerm", "useConditionOccurrenceShortTerm",
        "useConditionOccurrencePrimaryInpatientAnyTimePrior",
        "useConditionOccurrencePrimaryInpatientLongTerm",
        "useConditionOccurrencePrimaryInpatientMediumTerm",
        "useConditionOccurrencePrimaryInpatientShortTerm",
        "useConditionEraAnyTimePrior", "useConditionEraLongTerm",
        "useConditionEraMediumTerm", "useConditionEraShortTerm",
        "useConditionEraOverlapping", "useConditionEraStartLongTerm",
        "useConditionEraStartMediumTerm", "useConditionEraStartShortTerm",
        "useConditionGroupEraAnyTimePrior", "useConditionGroupEraLongTerm",
        "useConditionGroupEraMediumTerm", "useConditionGroupEraShortTerm",
        "useConditionGroupEraOverlapping", "useConditionGroupEraStartLongTerm",
        "useConditionGroupEraStartMediumTerm", "useConditionGroupEraStartShortTerm",
        "useDrugExposureAnyTimePrior", "useDrugExposureLongTerm",
        "useDrugExposureMediumTerm", "useDrugExposureShortTerm",
        "useDrugEraAnyTimePrior", "useDrugEraLongTerm",
        "useDrugEraMediumTerm", "useDrugEraShortTerm",
        "useDrugEraOverlapping", "useDrugEraStartLongTerm",
        "useDrugEraStartMediumTerm", "useDrugEraStartShortTerm",
        "useDrugGroupEraAnyTimePrior", "useDrugGroupEraLongTerm",
        "useDrugGroupEraMediumTerm", "useDrugGroupEraShortTerm",
        "useDrugGroupEraOverlapping", "useDrugGroupEraStartLongTerm",
        "useDrugGroupEraStartMediumTerm", "useDrugGroupEraStartShortTerm",
        "useProcedureOccurrenceAnyTimePrior", "useProcedureOccurrenceLongTerm",
        "useProcedureOccurrenceMediumTerm", "useProcedureOccurrenceShortTerm",
        "useDeviceExposureAnyTimePrior", "useDeviceExposureLongTerm",
        "useDeviceExposureMediumTerm", "useDeviceExposureShortTerm",
        "useMeasurementAnyTimePrior", "useMeasurementLongTerm",
        "useMeasurementMediumTerm", "useMeasurementShortTerm",
        "useMeasurementValueAnyTimePrior", "useMeasurementValueLongTerm",
        "useMeasurementValueMediumTerm", "useMeasurementValueShortTerm",
        "useMeasurementRangeGroupAnyTimePrior", "useMeasurementRangeGroupLongTerm",
        "useMeasurementRangeGroupMediumTerm", "useMeasurementRangeGroupShortTerm",
        "useMeasurementValueAsConceptAnyTimePrior", "useMeasurementValueAsConceptLongTerm",
        "useMeasurementValueAsConceptMediumTerm", "useMeasurementValueAsConceptShortTerm",
        "useObservationAnyTimePrior", "useObservationLongTerm",
        "useObservationMediumTerm", "useObservationShortTerm",
        "useObservationValueAsConceptAnyTimePrior", "useObservationValueAsConceptLongTerm",
        "useObservationValueAsConceptMediumTerm", "useObservationValueAsConceptShortTerm",
        "useCharlsonIndex", "useDcsi", "useChads2", "useChads2Vasc", "useHfrs",
        "useDistinctConditionCountLongTerm", "useDistinctConditionCountMediumTerm",
        "useDistinctConditionCountShortTerm", "useDistinctIngredientCountLongTerm",
        "useDistinctIngredientCountMediumTerm", "useDistinctIngredientCountShortTerm",
        "useDistinctProcedureCountLongTerm", "useDistinctProcedureCountMediumTerm",
        "useDistinctProcedureCountShortTerm", "useDistinctMeasurementCountLongTerm",
        "useDistinctMeasurementCountMediumTerm", "useDistinctMeasurementCountShortTerm",
        "useDistinctObservationCountLongTerm", "useDistinctObservationCountMediumTerm",
        "useDistinctObservationCountShortTerm", "useVisitCountLongTerm",
        "useVisitCountMediumTerm", "useVisitCountShortTerm",
        "useVisitConceptCountLongTerm", "useVisitConceptCountMediumTerm",
        "useVisitConceptCountShortTerm",
    ]
    settings = {"temporal": False, "temporalSequence": False}
    any_use_true = False
    for flag in _USE_FLAGS:
        val = kwargs.get(flag, False)
        if val:
            settings[flag.replace("use", "", 1)] = True
            any_use_true = True
    if not any_use_true:
        raise ValueError("No covariate analysis selected. Must select at least one")
    settings["longTermStartDays"] = kwargs.get("longTermStartDays", -365)
    settings["mediumTermStartDays"] = kwargs.get("mediumTermStartDays", -180)
    settings["shortTermStartDays"] = kwargs.get("shortTermStartDays", -30)
    settings["endDays"] = kwargs.get("endDays", 0)
    settings["includedCovariateConceptIds"] = kwargs.get("includedCovariateConceptIds", [])
    settings["addDescendantsToInclude"] = kwargs.get("addDescendantsToInclude", False)
    settings["excludedCovariateConceptIds"] = kwargs.get("excludedCovariateConceptIds", [])
    settings["addDescendantsToExclude"] = kwargs.get("addDescendantsToExclude", False)
    settings["includedCovariateIds"] = kwargs.get("includedCovariateIds", [])
    settings["_fun"] = "getDbDefaultCovariateData"
    settings["_class"] = "covariateSettings"
    return settings


def create_default_covariate_settings(
    included_covariate_concept_ids: list = None,
    add_descendants_to_include: bool = False,
    excluded_covariate_concept_ids: list = None,
    add_descendants_to_exclude: bool = False,
    included_covariate_ids: list = None
) -> dict:
    """Create default covariate settings (common demographics + condition/drug/procedure)."""
    settings = _get_default_characterization_covariate_settings()
    settings["includedCovariateConceptIds"] = included_covariate_concept_ids or []
    settings["addDescendantsToInclude"] = add_descendants_to_include
    settings["excludedCovariateConceptIds"] = excluded_covariate_concept_ids or []
    settings["addDescendantsToExclude"] = add_descendants_to_exclude
    settings["includedCovariateIds"] = included_covariate_ids or []
    settings["_fun"] = "getDbDefaultCovariateData"
    return settings


def create_temporal_covariate_settings(**kwargs) -> dict:
    """Create temporal covariate settings for FeatureExtraction.

    Accepts temporal use* boolean flags (default False) plus:
    - temporal_start_days (default [-365, -30, 0, 1, 31])
    - temporal_end_days (default [-31, -1, 0, 30, 365])
    - included/excluded covariate concept/covariate IDs
    """
    _USE_FLAGS = [
        "useDemographicsGender", "useDemographicsAge", "useDemographicsAgeGroup",
        "useDemographicsRace", "useDemographicsEthnicity", "useDemographicsIndexYear",
        "useDemographicsIndexMonth", "useDemographicsPriorObservationTime",
        "useDemographicsPostObservationTime", "useDemographicsTimeInCohort",
        "useDemographicsIndexYearMonth",
        "useConditionEraGroupStart", "useConditionEraGroupOverlap",
        "useDrugEraGroupStart", "useDrugEraGroupOverlap",
        "useProcedureOccurrenceStart", "useProcedureOccurrenceOverlap",
        "useDeviceExposureStart", "useDeviceExposureOverlap",
        "useMeasurementStart", "useMeasurementOverlap",
        "useObservationStart", "useObservationOverlap",
        "useVisitCountStart", "useVisitCountOverlap",
        "useVisitConceptCountStart", "useVisitConceptCountOverlap",
        "useConditionOccurrenceStart", "useConditionOccurrenceOverlap",
        "useDrugExposureStart", "useDrugExposureOverlap",
        "useConditionEraStart", "useConditionEraOverlap",
    ]
    settings = {"temporal": True, "temporalSequence": False}
    any_use_true = False
    for flag in _USE_FLAGS:
        val = kwargs.get(flag, False)
        if val:
            settings[flag.replace("use", "", 1)] = True
            any_use_true = True
    if not any_use_true:
        raise ValueError("No covariate analysis selected. Must select at least one")
    settings["temporalStartDays"] = kwargs.get("temporalStartDays", [-365, -30, 0, 1, 31])
    settings["temporalEndDays"] = kwargs.get("temporalEndDays", [-31, -1, 0, 30, 365])
    settings["includedCovariateConceptIds"] = kwargs.get("includedCovariateConceptIds", [])
    settings["addDescendantsToInclude"] = kwargs.get("addDescendantsToInclude", False)
    settings["excludedCovariateConceptIds"] = kwargs.get("excludedCovariateConceptIds", [])
    settings["addDescendantsToExclude"] = kwargs.get("addDescendantsToExclude", False)
    settings["includedCovariateIds"] = kwargs.get("includedCovariateIds", [])
    settings["_fun"] = "getDbDefaultCovariateData"
    settings["_class"] = "covariateSettings"
    return settings


def create_detailed_covariate_settings(analyses: list = None) -> dict:
    """Create detailed covariate settings."""
    return {
        "temporal": False,
        "temporalSequence": False,
        "analyses": analyses or [],
        "_fun": "getDbCovariateData",
        "_class": "covariateSettings"
    }


def create_analysis_details(
    analysis_id: int,
    sql_file_name: str,
    parameters: dict = None,
    included_covariate_concept_ids: list = None,
    add_descendants_to_include: bool = False,
    excluded_covariate_concept_ids: list = None,
    add_descendants_to_exclude: bool = False,
    included_covariate_ids: list = None
) -> dict:
    """Create analysis details for detailed covariate settings."""
    return {
        "analysisId": analysis_id,
        "sqlFileName": sql_file_name,
        "parameters": parameters or {},
        "includedCovariateConceptIds": included_covariate_concept_ids or [],
        "addDescendantsToInclude": add_descendants_to_include,
        "excludedCovariateConceptIds": excluded_covariate_concept_ids or [],
        "addDescendantsToExclude": add_descendants_to_exclude,
        "includedCovariateIds": included_covariate_ids or [],
        "_class": "analysisDetail"
    }


# =============================================================================
# CohortMethod Builder Functions
# =============================================================================

def create_get_db_cohort_method_data_args(
    covariate_settings: Optional[dict] = None,
    remove_duplicate_subjects: str = "keep first, truncate to second",
    first_exposure_only: bool = True,
    washout_period: int = 365,
    nesting_cohort_id: Optional[int] = None,
    restrict_to_common_period: bool = True,
    min_age: Optional[int] = None,
    max_age: Optional[int] = None,
    gender_concept_ids: Optional[list] = None,
    study_start_date: str = "",
    study_end_date: str = "",
    max_cohort_size: int = 0
) -> dict:
    """Create arguments for getDbCohortMethodData."""
    if covariate_settings is None:
        covariate_settings = create_default_covariate_settings()
    return {
        "covariateSettings": covariate_settings,
        "removeDuplicateSubjects": remove_duplicate_subjects,
        "firstExposureOnly": first_exposure_only,
        "washoutPeriod": washout_period,
        "nestingCohortId": nesting_cohort_id,
        "restrictToCommonPeriod": restrict_to_common_period,
        "minAge": min_age,
        "maxAge": max_age,
        "genderConceptIds": gender_concept_ids,
        "studyStartDate": study_start_date,
        "studyEndDate": study_end_date,
        "maxCohortSize": max_cohort_size,
        "_class": "GetDbCohortMethodDataArgs"
    }


def create_create_study_population_args(
    remove_subjects_with_prior_outcome: bool = True,
    prior_outcome_lookback: int = 99999,
    min_days_at_risk: int = 1,
    max_days_at_risk: int = 99999,
    risk_window_start: int = 0,
    start_anchor: str = "cohort start",
    risk_window_end: int = 0,
    end_anchor: str = "cohort end",
    censor_at_new_risk_window: bool = False
) -> dict:
    """Create arguments for CohortMethod createStudyPopulation."""
    return {
        "removeSubjectsWithPriorOutcome": remove_subjects_with_prior_outcome,
        "priorOutcomeLookback": prior_outcome_lookback,
        "minDaysAtRisk": min_days_at_risk,
        "maxDaysAtRisk": max_days_at_risk,
        "riskWindowStart": risk_window_start,
        "startAnchor": start_anchor,
        "riskWindowEnd": risk_window_end,
        "endAnchor": end_anchor,
        "censorAtNewRiskWindow": censor_at_new_risk_window,
        "_class": "CreateStudyPopulationArgs"
    }


def create_create_ps_args(
    exclude_covariate_ids: list = None,
    include_covariate_ids: list = None,
    max_cohort_size_for_fitting: int = 250000,
    error_on_high_correlation: bool = True,
    stop_on_error: bool = True,
    prior: Optional[dict] = None,
    control: Optional[dict] = None,
    estimator: str = "att"
) -> dict:
    """Create arguments for createPs."""
    if prior is None:
        prior = create_prior(prior_type="laplace", exclude=[0], use_cross_validation=True)
    if control is None:
        control = create_control(noise_level="silent", cv_type="auto", seed=1,
                                 reset_coefficients=True, tolerance=2e-07,
                                 cv_repetitions=10, starting_variance=0.01)
    return {
        "excludeCovariateIds": exclude_covariate_ids or [],
        "includeCovariateIds": include_covariate_ids or [],
        "maxCohortSizeForFitting": max_cohort_size_for_fitting,
        "errorOnHighCorrelation": error_on_high_correlation,
        "stopOnError": stop_on_error,
        "prior": prior,
        "control": control,
        "estimator": estimator,
        "_class": "CreatePsArgs"
    }


def create_trim_by_ps_args(
    trim_fraction: Optional[float] = None,
    equipoise_bounds: Optional[list] = None,
    max_weight: Optional[float] = None,
    trim_method: str = "symmetric"
) -> dict:
    """Create arguments for trimByPs."""
    return {
        "trimFraction": trim_fraction,
        "equipoiseBounds": equipoise_bounds,
        "maxWeight": max_weight,
        "trimMethod": trim_method,
        "_class": "TrimByPsArgs"
    }


def create_truncate_iptw_args(max_weight: float = 10) -> dict:
    """Create arguments for truncateIptw."""
    return {"maxWeight": max_weight, "_class": "TruncateIptwArgs"}


def create_match_on_ps_args(
    caliper: float = 0.2,
    caliper_scale: str = "standardized logit",
    max_ratio: int = 1,
    allow_reverse_match: bool = False,
    match_columns: list = None,
    match_covariate_ids: list = None
) -> dict:
    """Create arguments for matchOnPs."""
    return {
        "caliper": caliper,
        "caliperScale": caliper_scale,
        "maxRatio": max_ratio,
        "allowReverseMatch": allow_reverse_match,
        "matchColumns": match_columns or [],
        "matchCovariateIds": match_covariate_ids or [],
        "_class": "MatchOnPsArgs"
    }


def create_stratify_by_ps_args(
    number_of_strata: int = 10,
    base_selection: str = "all",
    stratification_columns: list = None,
    stratification_covariate_ids: list = None
) -> dict:
    """Create arguments for stratifyByPs."""
    return {
        "numberOfStrata": number_of_strata,
        "baseSelection": base_selection,
        "stratificationColumns": stratification_columns or [],
        "stratificationCovariateIds": stratification_covariate_ids or [],
        "_class": "StratifyByPsArgs"
    }


def create_compute_covariate_balance_args(
    subgroup_covariate_id: Optional[int] = None,
    max_cohort_size: int = 250000,
    covariate_filter: Optional[list] = None,
    threshold: float = 0.1,
    alpha: float = 0.05
) -> dict:
    """Create arguments for computeCovariateBalance."""
    return {
        "subgroupCovariateId": subgroup_covariate_id,
        "maxCohortSize": max_cohort_size,
        "covariateFilter": covariate_filter,
        "threshold": threshold,
        "alpha": alpha,
        "_class": "ComputeCovariateBalanceArgs"
    }


def create_fit_outcome_model_args(
    model_type: str = "cox",
    stratified: bool = False,
    use_covariates: bool = False,
    inverse_pt_weighting: bool = False,
    bootstrap_ci: bool = False,
    bootstrap_replicates: int = 200,
    interaction_covariate_ids: list = None,
    exclude_covariate_ids: list = None,
    include_covariate_ids: list = None,
    profile_grid: Optional[list] = None,
    profile_bounds: Optional[list] = None,
    prior: Optional[dict] = None,
    control: Optional[dict] = None
) -> dict:
    """Create arguments for fitOutcomeModel."""
    if profile_bounds is None:
        profile_bounds = [math.log(0.1), math.log(10)]
    if prior is None:
        prior = create_prior(prior_type="laplace", use_cross_validation=True)
    if control is None:
        control = create_control(cv_type="auto", starting_variance=0.01,
                                 tolerance=2e-07, noise_level="silent")
    return {
        "modelType": model_type,
        "stratified": stratified,
        "useCovariates": use_covariates,
        "inversePtWeighting": inverse_pt_weighting,
        "bootstrapCi": bootstrap_ci,
        "bootstrapReplicates": bootstrap_replicates,
        "interactionCovariateIds": interaction_covariate_ids or [],
        "excludeCovariateIds": exclude_covariate_ids or [],
        "includeCovariateIds": include_covariate_ids or [],
        "profileGrid": profile_grid,
        "profileBounds": profile_bounds,
        "prior": prior,
        "control": control,
        "_class": "FitOutcomeModelArgs"
    }


def create_cm_analysis(
    analysis_id: int = 1,
    description: str = "",
    get_db_cohort_method_data_args: dict = None,
    create_study_pop_args: dict = None,
    create_ps_args: Optional[dict] = None,
    trim_by_ps_args: Optional[dict] = None,
    truncate_iptw_args: Optional[dict] = None,
    match_on_ps_args: Optional[dict] = None,
    stratify_by_ps_args: Optional[dict] = None,
    compute_shared_covariate_balance_args: Optional[dict] = None,
    compute_covariate_balance_args: Optional[dict] = None,
    fit_outcome_model_args: Optional[dict] = None
) -> dict:
    """Create a CohortMethod analysis specification."""
    return {
        "analysisId": analysis_id,
        "description": description,
        "getDbCohortMethodDataArgs": get_db_cohort_method_data_args,
        "createStudyPopArgs": create_study_pop_args,
        "createPsArgs": create_ps_args,
        "trimByPsArgs": trim_by_ps_args,
        "truncateIptwArgs": truncate_iptw_args,
        "matchOnPsArgs": match_on_ps_args,
        "stratifyByPsArgs": stratify_by_ps_args,
        "computeSharedCovariateBalanceArgs": compute_shared_covariate_balance_args,
        "computeCovariateBalanceArgs": compute_covariate_balance_args,
        "fitOutcomeModelArgs": fit_outcome_model_args,
        "_class": "CmAnalysis"
    }


def create_outcome(
    outcome_id: int,
    outcome_of_interest: bool = True,
    true_effect_size: float = None,
    prior_outcome_lookback: Optional[int] = None,
    risk_window_start: Optional[int] = None,
    start_anchor: Optional[str] = None,
    risk_window_end: Optional[int] = None,
    end_anchor: Optional[str] = None
) -> dict:
    """Create an outcome definition for CohortMethod."""
    return {
        "outcomeId": outcome_id,
        "outcomeOfInterest": outcome_of_interest,
        "trueEffectSize": true_effect_size if true_effect_size is not None else math.nan,
        "priorOutcomeLookback": prior_outcome_lookback,
        "riskWindowStart": risk_window_start,
        "startAnchor": start_anchor,
        "riskWindowEnd": risk_window_end,
        "endAnchor": end_anchor,
        "_class": "Outcome"
    }


def create_target_comparator_outcomes(
    target_id: int,
    comparator_id: int,
    outcomes: list,
    nesting_cohort_id: Optional[int] = None,
    excluded_covariate_concept_ids: list = None,
    included_covariate_concept_ids: list = None
) -> dict:
    """Create a target-comparator-outcomes specification."""
    return {
        "targetId": target_id,
        "comparatorId": comparator_id,
        "outcomes": outcomes,
        "nestingCohortId": nesting_cohort_id,
        "excludedCovariateConceptIds": excluded_covariate_concept_ids or [],
        "includedCovariateConceptIds": included_covariate_concept_ids or [],
        "_class": "TargetComparatorOutcomes"
    }


def create_cm_diagnostic_thresholds(
    mdrr_threshold: float = 10,
    ease_threshold: float = 0.25,
    sdm_threshold: float = 0.1,
    sdm_alpha: Optional[float] = None,
    equipoise_threshold: float = 0.2,
    generalizability_sdm_threshold: float = 999
) -> dict:
    """Create CohortMethod diagnostic thresholds."""
    return {
        "mdrrThreshold": mdrr_threshold,
        "easeThreshold": ease_threshold,
        "sdmThreshold": sdm_threshold,
        "sdmAlpha": sdm_alpha,
        "equipoiseThreshold": equipoise_threshold,
        "generalizabilitySdmThreshold": generalizability_sdm_threshold,
        "_class": "CmDiagnosticThresholds"
    }


# =============================================================================
# SelfControlledCaseSeries Builder Functions
# =============================================================================

def create_era_covariate_settings(
    include_era_ids: Union[list, str],
    exclude_era_ids: Optional[list] = None,
    label: str = "Covariates",
    stratify_by_id: bool = False,
    start: int = 0,
    start_anchor: str = "era start",
    end: int = 0,
    end_anchor: str = "era end",
    first_occurrence_only: bool = False,
    allow_regularization: bool = False,
    profile_likelihood: bool = False,
    exposure_of_interest: bool = False
) -> dict:
    """Create era covariate settings for SCCS."""
    return {
        "includeEraIds": include_era_ids,
        "excludeEraIds": exclude_era_ids,
        "label": label,
        "stratifyById": stratify_by_id,
        "start": start,
        "startAnchor": start_anchor,
        "end": end,
        "endAnchor": end_anchor,
        "firstOccurrenceOnly": first_occurrence_only,
        "allowRegularization": allow_regularization,
        "profileLikelihood": profile_likelihood,
        "exposureOfInterest": exposure_of_interest,
        "_class": "EraCovariateSettings"
    }


def create_age_covariate_settings(
    age_knots: int = 5,
    allow_regularization: bool = False,
    compute_confidence_intervals: bool = False
) -> dict:
    """Create age covariate settings for SCCS."""
    return {
        "ageKnots": age_knots,
        "allowRegularization": allow_regularization,
        "computeConfidenceIntervals": compute_confidence_intervals,
        "_class": "AgeCovariateSettings"
    }


def create_seasonality_covariate_settings(
    season_knots: int = 5,
    allow_regularization: bool = False,
    compute_confidence_intervals: bool = False
) -> dict:
    """Create seasonality covariate settings for SCCS."""
    return {
        "seasonKnots": season_knots,
        "allowRegularization": allow_regularization,
        "computeConfidenceIntervals": compute_confidence_intervals,
        "_class": "SeasonalityCovariateSettings"
    }


def create_calendar_time_covariate_settings(
    calendar_time_knots: int = 5,
    allow_regularization: bool = False,
    compute_confidence_intervals: bool = False
) -> dict:
    """Create calendar time covariate settings for SCCS."""
    return {
        "calendarTimeKnots": calendar_time_knots,
        "allowRegularization": allow_regularization,
        "computeConfidenceIntervals": compute_confidence_intervals,
        "_class": "CalendarTimeCovariateSettings"
    }


def create_control_interval_settings(
    include_era_ids: Optional[list] = None,
    exclude_era_ids: Optional[list] = None,
    start: int = 0,
    start_anchor: str = "era start",
    end: int = 0,
    end_anchor: str = "era end",
    first_occurrence_only: bool = False
) -> dict:
    """Create control interval settings for SCRI."""
    return {
        "includeEraIds": include_era_ids,
        "excludeEraIds": exclude_era_ids,
        "start": start,
        "startAnchor": start_anchor,
        "end": end,
        "endAnchor": end_anchor,
        "firstOccurrenceOnly": first_occurrence_only,
        "_class": "ControlIntervalSettings"
    }


def create_get_db_sccs_data_args(
    nesting_cohort_id: Optional[int] = None,
    delete_covariates_small_count: int = 0,
    study_start_dates: list = None,
    study_end_dates: list = None,
    max_cases_per_outcome: int = 0,
    exposure_ids: Union[str, list] = "exposureId",
    custom_covariate_ids: Optional[list] = None
) -> dict:
    """Create arguments for getDbSccsData."""
    return {
        "nestingCohortId": nesting_cohort_id,
        "deleteCovariatesSmallCount": delete_covariates_small_count,
        "studyStartDates": study_start_dates or [],
        "studyEndDates": study_end_dates or [],
        "maxCasesPerOutcome": max_cases_per_outcome,
        "exposureIds": exposure_ids,
        "customCovariateIds": custom_covariate_ids,
        "_class": "GetDbSccsDataArgs"
    }


def create_sccs_create_study_population_args(
    first_outcome_only: bool = False,
    naive_period: int = 0,
    min_age: Optional[float] = None,
    max_age: Optional[float] = None,
    gender_concept_ids: Optional[list] = None,
    restrict_time_to_era_id: Optional[int] = None
) -> dict:
    """Create study population args for SCCS (different from CohortMethod version)."""
    return {
        "firstOutcomeOnly": first_outcome_only,
        "naivePeriod": naive_period,
        "minAge": min_age,
        "maxAge": max_age,
        "genderConceptIds": gender_concept_ids,
        "restrictTimeToEraId": restrict_time_to_era_id,
        "_class": "CreateStudyPopulationArgs"
    }


def create_create_sccs_interval_data_args(
    era_covariate_settings: Union[dict, list],
    age_covariate_settings: Optional[dict] = None,
    seasonality_covariate_settings: Optional[dict] = None,
    calendar_time_covariate_settings: Optional[dict] = None,
    min_cases_for_time_covariates: int = 10000,
    end_of_observation_era_length: int = 30,
    event_dependent_observation: bool = False
) -> dict:
    """Create arguments for createSccsIntervalData."""
    return {
        "eraCovariateSettings": era_covariate_settings,
        "ageCovariateSettings": age_covariate_settings,
        "seasonalityCovariateSettings": seasonality_covariate_settings,
        "calendarTimeCovariateSettings": calendar_time_covariate_settings,
        "minCasesForTimeCovariates": min_cases_for_time_covariates,
        "endOfObservationEraLength": end_of_observation_era_length,
        "eventDependentObservation": event_dependent_observation,
        "_class": "CreateSccsIntervalDataArgs"
    }


def create_create_scri_interval_data_args(
    era_covariate_settings: Union[dict, list],
    control_interval_settings: dict
) -> dict:
    """Create arguments for createScriIntervalData."""
    return {
        "eraCovariateSettings": era_covariate_settings,
        "controlIntervalSettings": control_interval_settings,
        "_class": "CreateScriIntervalDataArgs"
    }


def create_fit_sccs_model_args(
    prior: Optional[dict] = None,
    control: Optional[dict] = None,
    profile_grid: Optional[list] = None,
    profile_bounds: Optional[list] = None
) -> dict:
    """Create arguments for fitSccsModel."""
    if prior is None:
        prior = create_prior("laplace", use_cross_validation=True)
    if control is None:
        control = create_control(cv_type="auto", selector_type="byPid",
                                 starting_variance=0.1, seed=1,
                                 reset_coefficients=True, noise_level="quiet")
    if profile_bounds is None:
        profile_bounds = [math.log(0.1), math.log(10)]
    return {
        "prior": prior,
        "control": control,
        "profileGrid": profile_grid,
        "profileBounds": profile_bounds,
        "_class": "FitSccsModelArgs"
    }


def create_sccs_analysis(
    analysis_id: int = 1,
    description: str = "",
    get_db_sccs_data_args: dict = None,
    create_study_population_args: dict = None,
    create_interval_data_args: dict = None,
    fit_sccs_model_args: dict = None
) -> dict:
    """Create an SCCS analysis specification."""
    return {
        "analysisId": analysis_id,
        "description": description,
        "getDbSccsDataArgs": get_db_sccs_data_args,
        "createStudyPopulationArgs": create_study_population_args,
        "createIntervalDataArgs": create_interval_data_args,
        "fitSccsModelArgs": fit_sccs_model_args,
        "_class": "SccsAnalysis"
    }


def create_exposure(
    exposure_id: int,
    exposure_id_ref: str = "exposureId",
    true_effect_size: float = None
) -> dict:
    """Create an exposure definition for SCCS."""
    return {
        "exposureId": exposure_id,
        "exposureIdRef": exposure_id_ref,
        "trueEffectSize": true_effect_size if true_effect_size is not None else math.nan,
        "_class": "Exposure"
    }


def create_exposures_outcome(
    outcome_id: int,
    exposures: list,
    nesting_cohort_id: Optional[int] = None
) -> dict:
    """Create an exposures-outcome combination for SCCS."""
    return {
        "outcomeId": outcome_id,
        "exposures": exposures,
        "nestingCohortId": nesting_cohort_id,
        "_class": "ExposuresOutcome"
    }


def create_sccs_diagnostic_thresholds(
    mdrr_threshold: float = 10,
    ease_threshold: float = 0.25,
    time_trend_max_ratio: float = 1.1,
    rare_outcome_max_prevalence: float = 0.1,
    event_observation_dependence_null_bounds: list = None,
    event_exposure_dependence_null_bounds: list = None
) -> dict:
    """Create SCCS diagnostic thresholds."""
    return {
        "mdrrThreshold": mdrr_threshold,
        "easeThreshold": ease_threshold,
        "timeTrendMaxRatio": time_trend_max_ratio,
        "rareOutcomeMaxPrevalence": rare_outcome_max_prevalence,
        "eventObservationDependenceNullBounds": event_observation_dependence_null_bounds or [0.5, 2.0],
        "eventExposureDependenceNullBounds": event_exposure_dependence_null_bounds or [0.8, 1.25],
        "_class": "SccsDiagnosticThresholds"
    }


def create_sccs_analyses_specifications(
    sccs_analysis_list: list,
    exposures_outcome_list: list,
    analyses_to_exclude: Optional[list] = None,
    combine_data_fetch_across_outcomes: bool = False,
    sccs_diagnostic_thresholds: Optional[dict] = None,
    control_type: str = "outcome"
) -> dict:
    """Create full SCCS analyses specifications."""
    if sccs_diagnostic_thresholds is None:
        sccs_diagnostic_thresholds = create_sccs_diagnostic_thresholds()
    return {
        "sccsAnalysisList": sccs_analysis_list,
        "exposuresOutcomeList": exposures_outcome_list,
        "analysesToExclude": analyses_to_exclude,
        "combineDataFetchAcrossOutcomes": combine_data_fetch_across_outcomes,
        "sccsDiagnosticThresholds": sccs_diagnostic_thresholds,
        "controlType": control_type,
        "_class": "SccsAnalysesSpecifications"
    }


# =============================================================================
# PatientLevelPrediction Builder Functions
# =============================================================================

def create_study_population_settings(
    binary: bool = True,
    include_all_outcomes: bool = True,
    first_exposure_only: bool = False,
    washout_period: int = 0,
    remove_subjects_with_prior_outcome: bool = True,
    prior_outcome_lookback: int = 99999,
    require_time_at_risk: bool = True,
    min_time_at_risk: int = 364,
    risk_window_start: int = 1,
    start_anchor: str = "cohort start",
    risk_window_end: int = 365,
    end_anchor: str = "cohort start",
    restrict_tar_to_cohort_end: bool = False
) -> dict:
    """Create PLP study population settings."""
    return {
        "binary": binary,
        "includeAllOutcomes": include_all_outcomes,
        "firstExposureOnly": first_exposure_only,
        "washoutPeriod": washout_period,
        "removeSubjectsWithPriorOutcome": remove_subjects_with_prior_outcome,
        "priorOutcomeLookback": prior_outcome_lookback,
        "requireTimeAtRisk": require_time_at_risk,
        "minTimeAtRisk": min_time_at_risk,
        "riskWindowStart": risk_window_start,
        "startAnchor": start_anchor,
        "riskWindowEnd": risk_window_end,
        "endAnchor": end_anchor,
        "restrictTarToCohortEnd": restrict_tar_to_cohort_end,
        "_class": "populationSettings"
    }


def create_restrict_plp_data_settings(
    study_start_date: str = "",
    study_end_date: str = "",
    first_exposure_only: bool = False,
    washout_period: int = 0,
    sample_size: Optional[int] = None
) -> dict:
    """Create PLP data restriction settings."""
    return {
        "studyStartDate": study_start_date,
        "studyEndDate": study_end_date,
        "firstExposureOnly": first_exposure_only,
        "washoutPeriod": washout_period,
        "sampleSize": sample_size,
        "_class": "restrictPlpDataSettings"
    }


def create_preprocess_settings(
    min_fraction: float = 0.001,
    normalize: bool = True,
    remove_redundancy: bool = True
) -> dict:
    """Create PLP preprocessing settings."""
    return {
        "minFraction": min_fraction,
        "normalize": normalize,
        "removeRedundancy": remove_redundancy,
        "_class": "preprocessSettings"
    }


def create_default_split_setting(
    test_fraction: float = 0.25,
    train_fraction: float = 0.75,
    split_seed: Optional[int] = None,
    nfold: int = 3,
    type: str = "stratified"
) -> dict:
    """Create PLP split settings."""
    import random as _random
    if split_seed is None:
        split_seed = _random.randint(1, 100000)
    fun_map = {"stratified": "randomSplitter", "time": "timeSplitter", "subject": "subjectSplitter"}
    return {
        "test": test_fraction,
        "train": train_fraction,
        "seed": split_seed,
        "nfold": nfold,
        "_fun": fun_map.get(type, "randomSplitter"),
        "_class": "splitSettings"
    }


def create_sample_settings(
    type: str = "none",
    number_outcomes_to_non_outcomes: int = 1,
    sample_seed: Optional[int] = None
) -> dict:
    """Create PLP sample settings."""
    import random as _random
    if sample_seed is None:
        sample_seed = _random.randint(1, 10000)
    fun_map = {"none": "sameData", "underSample": "underSampleData", "overSample": "overSampleData"}
    return {
        "numberOutcomestoNonOutcomes": number_outcomes_to_non_outcomes,
        "sampleSeed": 1 if type == "none" else sample_seed,
        "_fun": fun_map.get(type, "sameData"),
        "_class": "sampleSettings"
    }


def create_feature_engineering_settings(type: str = "none") -> dict:
    """Create PLP feature engineering settings."""
    return {
        "_fun": "sameData" if type == "none" else type,
        "_class": "featureEngineeringSettings"
    }


def create_univariate_feature_selection(k: int = 100) -> dict:
    """Create univariate feature selection settings."""
    return {
        "k": k,
        "_fun": "univariateFeatureSelection",
        "_class": "featureEngineeringSettings"
    }


def create_random_forest_feature_selection(ntrees: int = 2000, max_depth: int = 17) -> dict:
    """Create random forest feature selection settings."""
    return {
        "ntrees": ntrees,
        "maxDepth": max_depth,
        "_fun": "randomForestFeatureSelection",
        "_class": "featureEngineeringSettings"
    }


def create_hyperparameter_settings(
    search: str = "grid",
    tuning_metric: Any = None,
    sample_size: Optional[int] = None,
    random_seed: Optional[int] = None,
    generator: Any = None
) -> dict:
    """Create PLP hyperparameter settings."""
    return {
        "search": search,
        "tuningMetric": tuning_metric,
        "sampleSize": sample_size,
        "randomSeed": random_seed,
        "generator": generator,
        "_class": "hyperparameterSettings"
    }


def create_cohort_covariate_settings(
    cohort_name: str,
    setting_id: int,
    cohort_id: int,
    cohort_database_schema: Optional[str] = None,
    cohort_table: Optional[str] = None,
    start_day: int = -30,
    end_day: int = 0,
    count: bool = False,
    age_interaction: bool = False,
    ln_age_interaction: bool = False,
    analysis_id: int = 456
) -> dict:
    """Create PLP cohort covariate settings."""
    return {
        "covariateName": cohort_name,
        "covariateId": cohort_id * 100000 + setting_id * 1000 + analysis_id,
        "cohortDatabaseSchema": cohort_database_schema,
        "cohortTable": cohort_table,
        "cohortIds": cohort_id,
        "startDay": start_day,
        "endDays": end_day,
        "count": count,
        "ageInteraction": age_interaction,
        "lnAgeInteraction": ln_age_interaction,
        "analysisId": analysis_id,
        "_fun": "PatientLevelPrediction::getCohortCovariateData",
        "_class": "covariateSettings"
    }


def create_model_design(
    target_id: Optional[int] = None,
    outcome_id: Optional[int] = None,
    restrict_plp_data_settings: Optional[dict] = None,
    population_settings: Optional[dict] = None,
    covariate_settings: Optional[dict] = None,
    feature_engineering_settings: Optional[list] = None,
    sample_settings: Optional[list] = None,
    preprocess_settings: Optional[dict] = None,
    model_settings: Optional[dict] = None,
    split_settings: Optional[dict] = None,
    hyperparameter_settings: Optional[dict] = None,
    run_covariate_summary: bool = True
) -> dict:
    """Create a PLP model design."""
    if restrict_plp_data_settings is None:
        restrict_plp_data_settings = create_restrict_plp_data_settings()
    if population_settings is None:
        population_settings = create_study_population_settings()
    if covariate_settings is None:
        covariate_settings = create_default_covariate_settings()
    if feature_engineering_settings is None:
        feature_engineering_settings = [create_feature_engineering_settings(type="none")]
    if sample_settings is None:
        sample_settings = [create_sample_settings(type="none")]
    if preprocess_settings is None:
        preprocess_settings = create_preprocess_settings()
    if split_settings is None:
        split_settings = create_default_split_setting()
    if hyperparameter_settings is None:
        hyperparameter_settings = create_hyperparameter_settings()
    return {
        "targetId": target_id,
        "outcomeId": outcome_id,
        "restrictPlpDataSettings": restrict_plp_data_settings,
        "covariateSettings": covariate_settings,
        "populationSettings": population_settings,
        "sampleSettings": sample_settings,
        "featureEngineeringSettings": feature_engineering_settings,
        "preprocessSettings": preprocess_settings,
        "modelSettings": model_settings,
        "splitSettings": split_settings,
        "hyperparameterSettings": hyperparameter_settings,
        "runCovariateSummary": run_covariate_summary,
        "_class": "modelDesign"
    }


def create_validation_design(
    target_id: int,
    outcome_id: int,
    population_settings: Optional[dict] = None,
    restrict_plp_data_settings: Optional[dict] = None,
    plp_model_list: list = None,
    recalibrate: Optional[str] = None,
    run_covariate_summary: bool = True
) -> dict:
    """Create a PLP validation design."""
    return {
        "targetId": target_id,
        "outcomeId": outcome_id,
        "populationSettings": population_settings,
        "plpModelList": plp_model_list or [],
        "restrictPlpDataSettings": restrict_plp_data_settings,
        "recalibrate": recalibrate,
        "runCovariateSummary": run_covariate_summary,
        "_class": "validationDesign"
    }


# PLP Model Settings (set_* functions)

def _make_cyclops_model_settings(model_name, model_type, cyclops_model_type,
                                  prior_type, param, seed, threads, tolerance,
                                  max_iterations, selector_type="byPid",
                                  add_intercept=True, prior_coefs=None):
    """Internal helper for Cyclops-based model settings."""
    settings = {
        "modelName": model_name, "modelType": model_type,
        "cyclopsModelType": cyclops_model_type,
        "priorfunction": "Cyclops::createPrior",
        "selectorType": selector_type, "crossValidationInPrior": True,
        "addIntercept": add_intercept, "useControl": True,
        "seed": seed, "threads": threads, "tolerance": tolerance,
        "cvRepetitions": 1, "maxIterations": max_iterations,
        "saveType": "RtoJson", "predict": "predictCyclops"
    }
    return {
        "fitFunction": "fitCyclopsModel",
        "param": param,
        "settings": settings,
        "_class": "modelSettings"
    }


def set_lasso_logistic_regression(
    variance: float = 0.01, seed: Optional[int] = None,
    include_covariate_ids: list = None, no_shrinkage: list = None,
    threads: int = -1, force_intercept: bool = False,
    upper_limit: float = 20, lower_limit: float = 0.01,
    tolerance: float = 2e-06, max_iterations: int = 3000,
    prior_coefs: Any = None
) -> dict:
    """Create settings for lasso logistic regression."""
    import random as _random
    if seed is None:
        seed = _random.randint(1, 100000000)
    param = {
        "priorParams": {"priorType": "laplace", "forceIntercept": force_intercept,
                        "variance": variance, "exclude": no_shrinkage or [0]},
        "includeCovariateIds": include_covariate_ids or [],
        "upperLimit": upper_limit, "lowerLimit": lower_limit,
        "priorCoefs": prior_coefs
    }
    return _make_cyclops_model_settings(
        "lassoLogisticRegression", "binary", "logistic", "laplace",
        param, seed, threads, tolerance, max_iterations
    )


def set_ridge_regression(
    variance: float = 0.01, seed: Optional[int] = None,
    include_covariate_ids: list = None, no_shrinkage: list = None,
    threads: int = -1, force_intercept: bool = False,
    upper_limit: float = 20, lower_limit: float = 0.01,
    tolerance: float = 2e-06, max_iterations: int = 3000,
    prior_coefs: Any = None
) -> dict:
    """Create settings for ridge regression."""
    import random as _random
    if seed is None:
        seed = _random.randint(1, 100000000)
    param = {
        "priorParams": {"priorType": "normal", "forceIntercept": force_intercept,
                        "variance": variance, "exclude": no_shrinkage or [0]},
        "includeCovariateIds": include_covariate_ids or [],
        "upperLimit": upper_limit, "lowerLimit": lower_limit,
        "priorCoefs": prior_coefs
    }
    return _make_cyclops_model_settings(
        "ridgeLogisticRegression", "binary", "logistic", "normal",
        param, seed, threads, tolerance, max_iterations
    )


def set_cox_model(
    variance: float = 0.01, seed: Optional[int] = None,
    include_covariate_ids: list = None, no_shrinkage: list = None,
    threads: int = -1, upper_limit: float = 20, lower_limit: float = 0.01,
    tolerance: float = 2e-07, max_iterations: int = 3000
) -> dict:
    """Create settings for Cox model."""
    import random as _random
    if seed is None:
        seed = _random.randint(1, 100000000)
    param = {
        "priorParams": {"priorType": "laplace", "variance": variance,
                        "exclude": no_shrinkage or []},
        "includeCovariateIds": include_covariate_ids or [],
        "upperLimit": upper_limit, "lowerLimit": lower_limit
    }
    return _make_cyclops_model_settings(
        "coxLasso", "survival", "cox", "laplace",
        param, seed, threads, tolerance, max_iterations,
        selector_type="byRow", add_intercept=False
    )


def set_iterative_hard_thresholding(
    K: int = 10, penalty: str = "bic", seed: Optional[int] = None,
    exclude: list = None, force_intercept: bool = False,
    fit_best_subset: bool = False, initial_ridge_variance: float = 0.1,
    tolerance: float = 1e-08, max_iterations: int = 10000,
    threshold: float = 1e-06, delta: float = 0
) -> dict:
    """Create settings for iterative hard thresholding."""
    import random as _random
    if seed is None:
        seed = _random.randint(1, 100000000)
    param = {
        "priorParams": {"K": K, "penalty": penalty, "exclude": exclude or [],
                        "forceIntercept": force_intercept},
        "fitBestSubset": fit_best_subset,
        "initialRidgeVariance": initial_ridge_variance,
        "tolerance": tolerance, "maxIterations": max_iterations,
        "threshold": threshold, "delta": delta
    }
    return {
        "fitFunction": "fitCyclopsModel",
        "param": param,
        "settings": {"modelName": "iterativeHardThresholding", "modelType": "binary",
                     "seed": seed, "saveType": "RtoJson", "predict": "predictCyclops"},
        "_class": "modelSettings"
    }


def _make_sklearn_model_settings(model_name, python_module, python_class, param, seed):
    """Internal helper for sklearn-based model settings."""
    return {
        "param": param,
        "settings": {
            "modelType": "binary", "seed": seed, "modelName": model_name,
            "pythonModule": python_module, "pythonClass": python_class,
            "saveType": "saveLoadSklearn", "predict": "predictSklearn"
        },
        "_class": "modelSettings"
    }


def set_gradient_boosting_machine(
    ntrees: list = None, nthread: int = 20, early_stop_round: int = 25,
    max_depth: list = None, min_child_weight: int = 1,
    learn_rate: list = None, scale_pos_weight: float = 1,
    lambda_: float = 1, alpha: float = 0,
    seed: Optional[int] = None
) -> dict:
    """Create settings for gradient boosting machine (XGBoost)."""
    import random as _random
    if seed is None:
        seed = _random.randint(1, 10000000)
    param = {
        "ntrees": ntrees or [100, 300], "nthread": nthread,
        "earlyStopRound": early_stop_round,
        "maxDepth": max_depth or [4, 6, 8],
        "minChildWeight": min_child_weight,
        "learnRate": learn_rate or [0.05, 0.1, 0.3],
        "scalePosWeight": scale_pos_weight,
        "lambda": lambda_, "alpha": alpha, "seed": [seed]
    }
    return {
        "fitFunction": "fitXgboost",
        "param": param,
        "settings": {"modelType": "binary", "seed": seed,
                     "modelName": "gradientBoostingMachine",
                     "saveType": "xgboost", "predict": "predictXgboost"},
        "_class": "modelSettings"
    }


def set_light_gbm(
    nthread: int = 20, early_stop_round: int = 25,
    num_iterations: list = None, num_leaves: list = None,
    max_depth: list = None, min_data_in_leaf: list = None,
    learning_rate: list = None, lambda_l1: list = None,
    lambda_l2: list = None, scale_pos_weight: float = 1,
    is_unbalance: bool = False, seed: Optional[int] = None
) -> dict:
    """Create settings for LightGBM."""
    import random as _random
    if seed is None:
        seed = _random.randint(1, 10000000)
    param = {
        "nthread": nthread, "earlyStopRound": early_stop_round,
        "numIterations": num_iterations or [100],
        "numLeaves": num_leaves or [31],
        "maxDepth": max_depth or [5, 10],
        "minDataInLeaf": min_data_in_leaf or [20],
        "learningRate": learning_rate or [0.05, 0.1, 0.3],
        "lambdaL1": lambda_l1 or [0], "lambdaL2": lambda_l2 or [0],
        "scalePosWeight": scale_pos_weight,
        "isUnbalance": is_unbalance, "seed": [seed]
    }
    return {
        "fitFunction": "fitLightGBM",
        "param": param,
        "settings": {"modelType": "binary", "seed": seed,
                     "modelName": "lightGBM",
                     "saveType": "lightgbm", "predict": "predictLightGBM"},
        "_class": "modelSettings"
    }


def set_ada_boost(
    n_estimators: list = None, learning_rate: list = None,
    seed: Optional[int] = None
) -> dict:
    """Create settings for AdaBoost."""
    import random as _random
    if seed is None:
        seed = _random.randint(1, 1000000)
    param = {
        "nEstimators": n_estimators or [10, 50, 200],
        "learningRate": learning_rate or [1, 0.5, 0.1],
        "seed": [seed]
    }
    return _make_sklearn_model_settings(
        "adaboost", "sklearn.ensemble", "AdaBoostClassifier", param, seed
    )


def set_decision_tree(
    criterion: list = None, splitter: list = None,
    max_depth: list = None, min_samples_split: list = None,
    min_samples_leaf: list = None, min_weight_fraction_leaf: list = None,
    max_features: list = None, max_leaf_nodes: list = None,
    min_impurity_decrease: list = None, class_weight: list = None,
    seed: Optional[int] = None
) -> dict:
    """Create settings for decision tree."""
    import random as _random
    if seed is None:
        seed = _random.randint(1, 1000000)
    param = {
        "criterion": criterion or ["gini"],
        "splitter": splitter or ["best"],
        "maxDepth": max_depth or [4, 10, None],
        "minSamplesSplit": min_samples_split or [2, 10],
        "minSamplesLeaf": min_samples_leaf or [10, 50],
        "minWeightFractionLeaf": min_weight_fraction_leaf or [0],
        "maxFeatures": max_features or [100, "sqrt", None],
        "maxLeafNodes": max_leaf_nodes or [None],
        "minImpurityDecrease": min_impurity_decrease or [1e-7],
        "classWeight": class_weight or [None],
        "seed": [seed]
    }
    return _make_sklearn_model_settings(
        "decisionTree", "sklearn.tree", "DecisionTreeClassifier", param, seed
    )


def set_mlp(
    hidden_layer_sizes: list = None, activation: list = None,
    solver: list = None, alpha: list = None,
    batch_size: list = None, learning_rate: list = None,
    learning_rate_init: list = None, power_t: list = None,
    max_iter: list = None, shuffle: list = None,
    tol: list = None, warm_start: list = None,
    momentum: list = None, nesterovs_momentum: list = None,
    early_stopping: list = None, validation_fraction: list = None,
    beta_1: list = None, beta_2: list = None,
    epsilon: list = None, n_iter_no_change: list = None,
    seed: Optional[int] = None
) -> dict:
    """Create settings for MLP (multi-layer perceptron)."""
    import random as _random
    if seed is None:
        seed = _random.randint(1, 100000)
    param = {
        "hiddenLayerSizes": hidden_layer_sizes or [[100], [20]],
        "activation": activation or ["relu"],
        "solver": solver or ["adam"],
        "alpha": alpha or [0.3, 0.01, 0.0001, 0.000001],
        "batchSize": batch_size or ["auto"],
        "learningRate": learning_rate or ["constant"],
        "learningRateInit": learning_rate_init or [0.001],
        "powerT": power_t or [0.5],
        "maxIter": max_iter or [200, 100],
        "shuffle": shuffle or [True],
        "tol": tol or [0.0001],
        "warmStart": warm_start or [True],
        "momentum": momentum or [0.9],
        "nesterovsMomentum": nesterovs_momentum or [True],
        "earlyStopping": early_stopping or [False],
        "validationFraction": validation_fraction or [0.1],
        "beta1": beta_1 or [0.9],
        "beta2": beta_2 or [0.999],
        "epsilon": epsilon or [1e-8],
        "nIterNoChange": n_iter_no_change or [10],
        "seed": [seed]
    }
    return _make_sklearn_model_settings(
        "mlp", "sklearn.neural_network", "MLPClassifier", param, seed
    )


def set_naive_bayes() -> dict:
    """Create settings for naive Bayes."""
    return _make_sklearn_model_settings(
        "naiveBayes", "sklearn.naive_bayes", "GaussianNB", {"none": "true"}, 0
    )


def set_random_forest(
    ntrees: list = None, criterion: list = None,
    max_depth: list = None, min_samples_split: list = None,
    min_samples_leaf: list = None, min_weight_fraction_leaf: list = None,
    mtries: list = None, max_leaf_nodes: list = None,
    min_impurity_decrease: list = None, bootstrap: list = None,
    max_samples: list = None, oob_score: list = None,
    n_jobs: list = None, class_weight: list = None,
    seed: Optional[int] = None
) -> dict:
    """Create settings for random forest."""
    import random as _random
    if seed is None:
        seed = _random.randint(1, 100000)
    param = {
        "ntrees": ntrees or [100, 500],
        "criterion": criterion or ["gini"],
        "maxDepth": max_depth or [4, 10, 17],
        "minSamplesSplit": min_samples_split or [2, 5],
        "minSamplesLeaf": min_samples_leaf or [1, 10],
        "minWeightFractionLeaf": min_weight_fraction_leaf or [0],
        "mtries": mtries or ["sqrt", "log2"],
        "maxLeafNodes": max_leaf_nodes or [None],
        "minImpurityDecrease": min_impurity_decrease or [0],
        "bootstrap": bootstrap or [True],
        "maxSamples": max_samples or [None, 0.9],
        "oobScore": oob_score or [False],
        "nJobs": n_jobs or [None],
        "classWeight": class_weight or [None],
        "seed": [seed]
    }
    return _make_sklearn_model_settings(
        "randomForest", "sklearn.ensemble", "RandomForestClassifier", param, seed
    )


def set_svm(
    C: list = None, kernel: list = None,
    degree: list = None, gamma: list = None,
    coef0: list = None, shrinking: list = None,
    tol: list = None, class_weight: list = None,
    cache_size: int = 500, seed: Optional[int] = None
) -> dict:
    """Create settings for SVM."""
    import random as _random
    if seed is None:
        seed = _random.randint(1, 100000)
    param = {
        "C": C or [1, 0.9, 2, 0.1],
        "kernel": kernel or ["rbf"],
        "degree": degree or [1, 3, 5],
        "gamma": gamma or ["scale", 1e-4, 3e-5, 0.001, 0.01, 0.25],
        "coef0": coef0 or [0.0],
        "shrinking": shrinking or [True],
        "tol": tol or [0.001],
        "cacheSize": [cache_size],
        "classWeight": class_weight or [None],
        "seed": [seed]
    }
    return _make_sklearn_model_settings(
        "svm", "sklearn.svm", "SVC", param, seed
    )


# =============================================================================
# EvidenceSynthesis Builder Functions
# =============================================================================

def generate_bayesian_hma_settings(
    primary_effect_prior_std: float = 1.0,
    secondary_effect_prior_std: float = 1.0,
    global_exposure_effect_prior_mean: list = None,
    global_exposure_effect_prior_std: list = None,
    primary_effect_precision_prior: list = None,
    secondary_effect_precision_prior: list = None,
    error_precision_prior: list = None,
    error_precision_start_value: float = 1.0,
    include_source_effect: bool = True,
    include_exposure_effect: bool = True,
    exposure_effect_count: int = 1,
    separate_exposure_prior: bool = False,
    chain_length: int = 1100000,
    burn_in: int = 100000,
    sub_sample_frequency: int = 100
) -> dict:
    """Create Bayesian hierarchical meta-analysis settings."""
    return {
        "primaryEffectPriorStd": primary_effect_prior_std,
        "secondaryEffectPriorStd": secondary_effect_prior_std,
        "globalExposureEffectPriorMean": global_exposure_effect_prior_mean or [0.0],
        "globalExposureEffectPriorStd": global_exposure_effect_prior_std or [2.0],
        "primaryEffectPrecisionPrior": primary_effect_precision_prior or [1.0, 1.0],
        "secondaryEffectPrecisionPrior": secondary_effect_precision_prior or [1.0, 1.0],
        "errorPrecisionPrior": error_precision_prior or [1.0, 1.0],
        "errorPrecisionStartValue": error_precision_start_value,
        "includeSourceEffect": include_source_effect,
        "includeExposureEffect": include_exposure_effect,
        "exposureEffectCount": exposure_effect_count,
        "separateExposurePrior": separate_exposure_prior,
        "chainLength": chain_length,
        "burnIn": burn_in,
        "subSampleFrequency": sub_sample_frequency,
        "_class": "BayesianHMASettings"
    }


# =============================================================================
# CohortIncidence Builder Functions
# =============================================================================

def create_incidence_design(
    cohort_defs: Optional[list] = None,
    target_defs: Optional[list] = None,
    outcome_defs: Optional[list] = None,
    tars: Optional[list] = None,
    analysis_list: Optional[list] = None,
    concept_sets: Optional[list] = None,
    subgroups: Optional[list] = None,
    strata_settings: Optional[dict] = None,
    study_window: Optional[dict] = None
) -> dict:
    """Create an incidence design for CohortIncidence."""
    design = {"_class": "IncidenceDesign"}
    if cohort_defs is not None:
        design["cohortDefs"] = cohort_defs
    if target_defs is not None:
        design["targetDefs"] = target_defs
    if outcome_defs is not None:
        design["outcomeDefs"] = outcome_defs
    if tars is not None:
        design["timeAtRiskDefs"] = tars
    if analysis_list is not None:
        design["analysisList"] = analysis_list
    if concept_sets is not None:
        design["conceptSets"] = concept_sets
    if subgroups is not None:
        design["subgroups"] = subgroups
    if strata_settings is not None:
        design["strataSettings"] = strata_settings
    if study_window is not None:
        design["studyWindow"] = study_window
    return design


def create_incidence_analysis(targets: list, outcomes: list, tars: list) -> dict:
    """Create an incidence analysis."""
    return {
        "targets": targets,
        "outcomes": outcomes,
        "tars": tars,
        "_class": "IncidenceAnalysis"
    }


def create_cohort_ref(id: int, name: str, description: Optional[str] = None) -> dict:
    """Create a cohort reference."""
    ref = {"id": id, "name": name, "_class": "CohortReference"}
    if description is not None:
        ref["description"] = description
    return ref


def create_outcome_def(
    id: int, name: Optional[str] = None,
    cohort_id: int = 0, clean_window: int = 0,
    exclude_cohort_id: Optional[int] = None
) -> dict:
    """Create an outcome definition for CohortIncidence."""
    result = {"id": id, "cohortId": cohort_id, "cleanWindow": clean_window,
              "_class": "Outcome"}
    if name is not None:
        result["name"] = name
    if exclude_cohort_id is not None:
        result["excludeCohortId"] = exclude_cohort_id
    return result


def create_time_at_risk_def(
    id: int, start_with: str = "start", start_offset: int = 0,
    end_with: str = "end", end_offset: int = 0
) -> dict:
    """Create a time-at-risk definition."""
    return {
        "id": id,
        "startWith": start_with,
        "startOffset": start_offset,
        "endWith": end_with,
        "endOffset": end_offset,
        "_class": "TimeAtRisk"
    }


def create_cohort_subgroup(
    id: int, name: str,
    description: Optional[str] = None,
    cohort_ref: Optional[dict] = None
) -> dict:
    """Create a cohort subgroup."""
    result = {"id": id, "name": name, "_class": "CohortSubgroup"}
    if description is not None:
        result["description"] = description
    if cohort_ref is not None:
        result["cohort"] = cohort_ref
    return result


def create_strata_settings(
    by_age: bool = False, by_gender: bool = False, by_year: bool = False,
    age_breaks: Optional[list] = None, age_break_list: Optional[list] = None
) -> dict:
    """Create strata settings for CohortIncidence."""
    if by_age and age_breaks is None and age_break_list is None:
        raise ValueError("When by_age=True, age_breaks or age_break_list must be provided")
    result = {"byAge": by_age, "byGender": by_gender, "byYear": by_year,
              "_class": "StrataSettings"}
    if age_breaks is not None:
        result["ageBreaks"] = age_breaks
    if age_break_list is not None:
        result["ageBreakList"] = age_break_list
    return result


def create_date_range(
    start_date: Optional[str] = None, end_date: Optional[str] = None
) -> dict:
    """Create a date range."""
    result = {"_class": "DateRange"}
    if start_date is not None:
        result["startDate"] = start_date
    if end_date is not None:
        result["endDate"] = end_date
    return result


# =============================================================================
# Characterization Builder Functions
# =============================================================================

def create_characterization_settings(
    time_to_event_settings: Optional[Union[dict, list]] = None,
    dechallenge_rechallenge_settings: Optional[Union[dict, list]] = None,
    aggregate_covariate_settings: Optional[Union[dict, list]] = None
) -> dict:
    """Create characterization settings."""
    if isinstance(time_to_event_settings, dict):
        time_to_event_settings = [time_to_event_settings]
    if isinstance(dechallenge_rechallenge_settings, dict):
        dechallenge_rechallenge_settings = [dechallenge_rechallenge_settings]
    if isinstance(aggregate_covariate_settings, dict):
        aggregate_covariate_settings = [aggregate_covariate_settings]
    return {
        "timeToEventSettings": time_to_event_settings,
        "dechallengeRechallengeSettings": dechallenge_rechallenge_settings,
        "aggregateCovariateSettings": aggregate_covariate_settings,
        "_class": "characterizationSettings"
    }


def create_dechallenge_rechallenge_settings(
    target_ids: list, outcome_ids: list,
    dechallenge_stop_interval: int = 30,
    dechallenge_evaluation_window: int = 30
) -> dict:
    """Create dechallenge-rechallenge settings."""
    return {
        "targetIds": target_ids,
        "outcomeIds": outcome_ids,
        "dechallengeStopInterval": dechallenge_stop_interval,
        "dechallengeEvaluationWindow": dechallenge_evaluation_window,
        "_class": "dechallengeRechallengeSettings"
    }


def create_time_to_event_settings(target_ids: list, outcome_ids: list) -> dict:
    """Create time-to-event settings."""
    return {
        "targetIds": target_ids,
        "outcomeIds": outcome_ids,
        "_class": "timeToEventSettings"
    }


def create_aggregate_covariate_settings(
    target_ids: list, outcome_ids: list,
    min_prior_observation: int = 0,
    outcome_washout_days: int = 0,
    risk_window_start: int = 1,
    start_anchor: str = "cohort start",
    risk_window_end: int = 365,
    end_anchor: str = "cohort start",
    covariate_settings: Optional[dict] = None,
    case_covariate_settings: Optional[dict] = None,
    case_pre_target_duration: int = 365,
    case_post_outcome_duration: int = 365,
    extract_non_case_covariates: bool = True
) -> dict:
    """Create aggregate covariate settings."""
    if covariate_settings is None:
        covariate_settings = _get_default_characterization_covariate_settings()
    if case_covariate_settings is None:
        case_covariate_settings = _get_default_case_covariate_settings()
    if isinstance(covariate_settings, dict) and "_class" in covariate_settings:
        covariate_settings = [covariate_settings]
    return {
        "targetIds": target_ids,
        "outcomeIds": outcome_ids,
        "minPriorObservation": min_prior_observation,
        "outcomeWashoutDays": outcome_washout_days,
        "riskWindowStart": risk_window_start,
        "startAnchor": start_anchor,
        "riskWindowEnd": risk_window_end,
        "endAnchor": end_anchor,
        "covariateSettings": covariate_settings,
        "caseCovariateSettings": case_covariate_settings,
        "casePreTargetDuration": case_pre_target_duration,
        "casePostOutcomeDuration": case_post_outcome_duration,
        "extractNonCaseCovariates": extract_non_case_covariates,
        "_class": "aggregateCovariateSettings"
    }


def create_during_covariate_settings(**kwargs) -> dict:
    """Create during-cohort covariate settings for Characterization.

    Accepts use* boolean flags (default False) plus concept filtering params.
    """
    _USE_FLAGS = [
        "useConditionOccurrenceDuring", "useConditionOccurrencePrimaryInpatientDuring",
        "useConditionEraDuring", "useConditionGroupEraDuring",
        "useDrugExposureDuring", "useDrugEraDuring", "useDrugGroupEraDuring",
        "useProcedureOccurrenceDuring", "useDeviceExposureDuring",
        "useMeasurementDuring", "useObservationDuring",
        "useVisitCountDuring", "useVisitConceptCountDuring",
    ]
    settings = {"temporal": False, "temporalSequence": False}
    any_use_true = False
    for flag in _USE_FLAGS:
        val = kwargs.get(flag, False)
        if val:
            settings[flag.replace("use", "", 1)] = True
            any_use_true = True
    if not any_use_true:
        raise ValueError("No covariate analysis selected. Must select at least one")
    settings["includedCovariateConceptIds"] = kwargs.get("includedCovariateConceptIds", [])
    settings["addDescendantsToInclude"] = kwargs.get("addDescendantsToInclude", False)
    settings["excludedCovariateConceptIds"] = kwargs.get("excludedCovariateConceptIds", [])
    settings["addDescendantsToExclude"] = kwargs.get("addDescendantsToExclude", False)
    settings["includedCovariateIds"] = kwargs.get("includedCovariateIds", [])
    settings["_fun"] = "Characterization::getDbDuringCovariateData"
    settings["_class"] = "covariateSettings"
    return settings


# =============================================================================
# Core Public Functions (User Story 1)
# =============================================================================

def create_empty_analysis_specifications() -> AnalysisSpecifications:
    """
    Create an empty analysis specifications object.

    Returns:
        An AnalysisSpecifications object with empty shared_resources and module_specifications.

    Example:
        >>> spec = create_empty_analysis_specifications()
        >>> print(spec.to_json())
    """
    return AnalysisSpecifications()


def add_shared_resources(
    analysis_specifications: AnalysisSpecifications,
    shared_resources: dict
) -> AnalysisSpecifications:
    """
    Add shared resources to analysis specifications.

    Args:
        analysis_specifications: The analysis specifications to modify.
        shared_resources: A shared resources dictionary.

    Returns:
        The modified analysis specifications.

    Raises:
        TypeError: If inputs are not the correct type.
    """
    if not isinstance(analysis_specifications, AnalysisSpecifications):
        raise TypeError("analysis_specifications must be an AnalysisSpecifications object")
    if not isinstance(shared_resources, dict):
        raise TypeError("shared_resources must be a dictionary")

    analysis_specifications.shared_resources.append(shared_resources)
    return analysis_specifications


def add_module_specifications(
    analysis_specifications: AnalysisSpecifications,
    module_specifications: dict
) -> AnalysisSpecifications:
    """
    Add module specifications to analysis specifications.

    Args:
        analysis_specifications: The analysis specifications to modify.
        module_specifications: A module specifications dictionary.

    Returns:
        The modified analysis specifications.

    Raises:
        TypeError: If inputs are not the correct type.
    """
    if not isinstance(analysis_specifications, AnalysisSpecifications):
        raise TypeError("analysis_specifications must be an AnalysisSpecifications object")
    if not isinstance(module_specifications, dict):
        raise TypeError("module_specifications must be a dictionary")

    analysis_specifications.module_specifications.append(module_specifications)
    return analysis_specifications


# =============================================================================
# Shared Resource Functions (User Story 2)
# =============================================================================

def create_cohort_shared_resource_specifications(
    cohort_definition_set: list[dict]
) -> dict:
    """
    Create cohort shared resource specifications.

    Args:
        cohort_definition_set: A list of dictionaries with keys:
            cohortId, cohortName, sql, json.
            Optionally: isSubset, subsetParent, subsetDefinitionId.

    Returns:
        A shared resources dictionary for cohort definitions.

    Raises:
        ValueError: If cohort_definition_set is not properly defined.

    Example:
        >>> cohorts = [
        ...     {"cohortId": 1, "cohortName": "Target", "sql": "SELECT...", "json": "{}"},
        ...     {"cohortId": 2, "cohortName": "Outcome", "sql": "SELECT...", "json": "{}"}
        ... ]
        >>> shared = create_cohort_shared_resource_specifications(cohorts)
    """
    if not _is_cohort_definition_set(cohort_definition_set):
        raise ValueError(
            "cohort_definition_set is not properly defined. "
            "Required keys: cohortId, cohortName, sql, json"
        )

    # Check for subset definitions
    has_subsets = any(
        row.get("isSubset", False) for row in cohort_definition_set
    )

    if has_subsets:
        parent_cohorts = [r for r in cohort_definition_set if not r.get("isSubset", False)]
    else:
        parent_cohorts = cohort_definition_set

    shared_resource = {
        "cohortDefinitions": _listafy(parent_cohorts),
        "_class": ("CohortDefinitionSharedResources", "SharedResources")
    }

    if has_subsets:
        subset_cohorts = [r for r in cohort_definition_set if r.get("isSubset", False)]
        subset_id_mapping = []
        for row in subset_cohorts:
            id_mapping = {
                "cohortId": row["cohortId"],
                "subsetId": row.get("subsetDefinitionId"),
                "targetCohortId": row.get("subsetParent")
            }
            subset_id_mapping.append(id_mapping)
        shared_resource["cohortSubsets"] = subset_id_mapping

    return shared_resource


def create_negative_control_outcome_cohort_shared_resource_specifications(
    negative_control_outcome_cohort_set: list[dict],
    occurrence_type: str,
    detect_on_descendants: bool
) -> dict:
    """
    Create negative control outcome cohort shared resource specifications.

    Args:
        negative_control_outcome_cohort_set: List of dicts with cohortId, cohortName, outcomeConceptId.
        occurrence_type: Either "first" or "all".
        detect_on_descendants: Whether to detect on descendant concepts.

    Returns:
        A shared resources dictionary for negative control outcomes.
    """
    return {
        "negativeControlOutcomes": {
            "negativeControlOutcomeCohortSet": negative_control_outcome_cohort_set,
            "occurrenceType": occurrence_type,
            "detectOnDescendants": detect_on_descendants
        },
        "_class": ("NegativeControlOutcomeSharedResources", "SharedResources")
    }


# =============================================================================
# Module Specification Functions (User Story 3)
# =============================================================================

def create_cohort_generator_module_specifications(
    generate_stats: bool = True
) -> dict:
    """Create CohortGenerator module specifications."""
    return {
        "module": "CohortGeneratorModule",
        "settings": {
            "generateStats": generate_stats
        },
        "_class": ("ModuleSpecifications", "CohortGeneratorModuleSpecifications")
    }


def create_cohort_diagnostics_module_specifications(
    cohort_ids: Optional[list[int]] = None,
    run_inclusion_statistics: bool = True,
    run_included_source_concepts: bool = True,
    run_orphan_concepts: bool = True,
    run_time_series: bool = False,
    run_visit_context: bool = True,
    run_breakdown_index_events: bool = True,
    run_incidence_rate: bool = True,
    run_cohort_relationship: bool = True,
    run_temporal_cohort_characterization: bool = True,
    temporal_covariate_settings: Optional[dict] = None,
    min_characterization_mean: float = 0.01,
    ir_washout_period: int = 0
) -> dict:
    """Create CohortDiagnostics module specifications."""
    if temporal_covariate_settings is None:
        temporal_covariate_settings = _get_default_temporal_covariate_settings()

    return {
        "module": "CohortDiagnosticsModule",
        "settings": {
            "cohortIds": cohort_ids,
            "runInclusionStatistics": run_inclusion_statistics,
            "runIncludedSourceConcepts": run_included_source_concepts,
            "runOrphanConcepts": run_orphan_concepts,
            "runTimeSeries": run_time_series,
            "runVisitContext": run_visit_context,
            "runBreakdownIndexEvents": run_breakdown_index_events,
            "runIncidenceRate": run_incidence_rate,
            "runCohortRelationship": run_cohort_relationship,
            "runTemporalCohortCharacterization": run_temporal_cohort_characterization,
            "temporalCovariateSettings": temporal_covariate_settings,
            "minCharacterizationMean": min_characterization_mean,
            "irWashoutPeriod": ir_washout_period
        },
        "_class": ("ModuleSpecifications", "CohortDiagnosticsModuleSpecifications")
    }


def create_cohort_incidence_module_specifications(
    ir_design: Optional[dict] = None
) -> dict:
    """Create CohortIncidence module specifications."""
    return {
        "module": "CohortIncidenceModule",
        "settings": {
            "irDesign": ir_design
        },
        "_class": ("ModuleSpecifications", "CohortIncidenceModuleSpecifications")
    }


def create_cohort_method_module_specifications(
    cm_analysis_list: list,
    target_comparator_outcomes_list: list,
    analyses_to_exclude: Optional[list] = None,
    refit_ps_for_every_outcome: bool = False,
    refit_ps_for_every_study_population: bool = True,
    cm_diagnostic_thresholds: Optional[dict] = None
) -> dict:
    """Create CohortMethod module specifications."""
    if cm_diagnostic_thresholds is None:
        cm_diagnostic_thresholds = _create_default_cm_diagnostic_thresholds()

    return {
        "module": "CohortMethodModule",
        "settings": {
            "cmAnalysisList": cm_analysis_list,
            "targetComparatorOutcomesList": target_comparator_outcomes_list,
            "analysesToExclude": analyses_to_exclude,
            "refitPsForEveryOutcome": refit_ps_for_every_outcome,
            "refitPsForEveryStudyPopulation": refit_ps_for_every_study_population,
            "cmDiagnosticThresholds": cm_diagnostic_thresholds
        },
        "_class": ("ModuleSpecifications", "CohortMethodModuleSpecifications")
    }


def create_characterization_module_specifications(
    target_ids: list[int],
    outcome_ids: list[int],
    outcome_washout_days: list[int] = None,
    min_prior_observation: int = 365,
    dechallenge_stop_interval: int = 30,
    dechallenge_evaluation_window: int = 30,
    risk_window_start: list[int] = None,
    start_anchor: list[str] = None,
    risk_window_end: list[int] = None,
    end_anchor: list[str] = None,
    min_characterization_mean: float = 0.01,
    covariate_settings: Optional[dict] = None,
    case_covariate_settings: Optional[dict] = None
) -> dict:
    """Create Characterization module specifications."""
    if outcome_washout_days is None:
        outcome_washout_days = [365]
    if risk_window_start is None:
        risk_window_start = [1, 1]
    if start_anchor is None:
        start_anchor = ["cohort start", "cohort start"]
    if risk_window_end is None:
        risk_window_end = [0, 365]
    if end_anchor is None:
        end_anchor = ["cohort end", "cohort end"]
    if covariate_settings is None:
        covariate_settings = _get_default_characterization_covariate_settings()
    if case_covariate_settings is None:
        case_covariate_settings = _get_default_case_covariate_settings()

    return {
        "module": "CharacterizationModule",
        "settings": {
            "targetIds": target_ids,
            "outcomeIds": outcome_ids,
            "outcomeWashoutDays": outcome_washout_days,
            "minPriorObservation": min_prior_observation,
            "dechallengeStopInterval": dechallenge_stop_interval,
            "dechallengeEvaluationWindow": dechallenge_evaluation_window,
            "riskWindowStart": risk_window_start,
            "startAnchor": start_anchor,
            "riskWindowEnd": risk_window_end,
            "endAnchor": end_anchor,
            "minCharacterizationMean": min_characterization_mean,
            "covariateSettings": covariate_settings,
            "caseCovariateSettings": case_covariate_settings
        },
        "_class": ("ModuleSpecifications", "CharacterizationModuleSpecifications")
    }


def create_patient_level_prediction_module_specifications(
    model_design_list: list,
    skip_diagnostics: bool = False
) -> dict:
    """Create PatientLevelPrediction module specifications."""
    return {
        "module": "PatientLevelPredictionModule",
        "settings": {
            "modelDesignList": model_design_list,
            "skipDiagnostics": skip_diagnostics
        },
        "_class": ("ModuleSpecifications", "PatientLevelPredictionModuleSpecifications")
    }


def create_patient_level_prediction_validation_module_specifications(
    validation_list: list
) -> dict:
    """Create PatientLevelPrediction Validation module specifications."""
    return {
        "module": "PatientLevelPredictionValidationModule",
        "settings": {
            "validationList": validation_list
        },
        "_class": ("ModuleSpecifications", "PatientLevelPredictionValidationModuleSpecifications")
    }


def create_self_controlled_case_series_module_specifications(
    sccs_analyses_specifications: dict
) -> dict:
    """Create SelfControlledCaseSeries module specifications."""
    return {
        "module": "SelfControlledCaseSeriesModule",
        "settings": {
            "sccsAnalysesSpecifications": sccs_analyses_specifications
        },
        "_class": ("ModuleSpecifications", "SelfControlledCaseSeriesModuleSpecifications")
    }


def create_evidence_synthesis_module_specifications(
    evidence_synthesis_analysis_list: list,
    es_diagnostic_thresholds: Optional[dict] = None
) -> dict:
    """Create EvidenceSynthesis module specifications."""
    if es_diagnostic_thresholds is None:
        es_diagnostic_thresholds = _create_default_es_diagnostic_thresholds()

    return {
        "module": "EvidenceSynthesisModule",
        "settings": {
            "evidenceSynthesisAnalysisList": evidence_synthesis_analysis_list,
            "esDiagnosticThresholds": es_diagnostic_thresholds
        },
        "_class": ("ModuleSpecifications", "EvidenceSynthesisModuleSpecifications")
    }


def create_treatment_patterns_module_specifications(
    cohorts: list[dict],
    include_treatments: Optional[list] = None,
    index_date_offset: Optional[int] = None,
    min_era_duration: int = 0,
    split_event_cohorts: Optional[list] = None,
    split_time: Optional[int] = None,
    era_collapse_size: int = 30,
    combination_window: int = 30,
    min_post_combination_duration: int = 30,
    filter_treatments: str = "First",
    max_path_length: int = 5,
    age_window: int = 5,
    min_cell_count: int = 1,
    censor_type: str = "minCellCount",
    overlap_method: str = "truncate",
    concat_targets: bool = True
) -> dict:
    """Create TreatmentPatterns module specifications."""
    return {
        "module": "TreatmentPatternsModule",
        "settings": {
            "cohorts": cohorts,
            "includeTreatments": include_treatments,
            "indexDateOffset": index_date_offset,
            "minEraDuration": min_era_duration,
            "splitEventCohorts": split_event_cohorts,
            "splitTime": split_time,
            "eraCollapseSize": era_collapse_size,
            "combinationWindow": combination_window,
            "minPostCombinationDuration": min_post_combination_duration,
            "filterTreatments": filter_treatments,
            "maxPathLength": max_path_length,
            "ageWindow": age_window,
            "minCellCount": min_cell_count,
            "censorType": censor_type,
            "overlapMethod": overlap_method,
            "concatTargets": concat_targets
        },
        "_class": ("ModuleSpecifications", "TreatmentPatternsModuleSpecifications")
    }


def create_cohort_survival_module_specifications(
    target_cohort_id: int,
    outcome_cohort_id: int,
    strata: Optional[list] = None,
    analysis_type: str = "single_event",
    competing_outcome_cohort_table: Optional[str] = None,
    competing_outcome_cohort_id: Optional[int] = None,
    outcome_date_variable: str = "cohort_start_date",
    outcome_washout: float = float('inf'),
    censor_on_cohort_exit: bool = False,
    censor_on_date: Optional[str] = None,
    follow_up_days: float = float('inf'),
    event_gap: int = 30,
    estimate_gap: int = 1,
    restricted_mean_follow_up: Optional[int] = None,
    minimum_survival_days: int = 1
) -> dict:
    """Create CohortSurvival module specifications."""
    return {
        "module": "CohortSurvivalModule",
        "settings": {
            "targetCohortId": target_cohort_id,
            "outcomeCohortId": outcome_cohort_id,
            "strata": strata,
            "analysisType": analysis_type,
            "competingOutcomeCohortTable": competing_outcome_cohort_table,
            "competingOutcomeCohortId": competing_outcome_cohort_id,
            "outcomeDateVariable": outcome_date_variable,
            "outcomeWashout": outcome_washout,
            "censorOnCohortExit": censor_on_cohort_exit,
            "censorOnDate": censor_on_date,
            "followUpDays": follow_up_days,
            "eventGap": event_gap,
            "estimateGap": estimate_gap,
            "restrictedMeanFollowUp": restricted_mean_follow_up,
            "minimumSurvivalDays": minimum_survival_days
        },
        "_class": ("ModuleSpecifications", "CohortSurvivalModuleSpecifications")
    }


# =============================================================================
# Convenience Functions
# =============================================================================

def to_json(analysis_specifications: AnalysisSpecifications, pretty: bool = True) -> str:
    """
    Serialize analysis specifications to JSON.

    Args:
        analysis_specifications: The specifications to serialize.
        pretty: Whether to format with indentation.

    Returns:
        JSON string.
    """
    return analysis_specifications.to_json(pretty=pretty)


def save_to_json(analysis_specifications: AnalysisSpecifications, filepath: str) -> None:
    """
    Save analysis specifications to a JSON file.

    Args:
        analysis_specifications: The specifications to save.
        filepath: Path to the output file.
    """
    with open(filepath, "w") as f:
        f.write(analysis_specifications.to_json(pretty=True))


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":
    # Example: Create a complete analysis specification

    # Step 1: Create empty specification
    spec = create_empty_analysis_specifications()

    # Step 2: Add cohort definitions
    cohort_definition_set = [
        {"cohortId": 1, "cohortName": "Type 2 Diabetes", "sql": "-- SQL", "json": "{}"},
        {"cohortId": 2, "cohortName": "Metformin Users", "sql": "-- SQL", "json": "{}"},
        {"cohortId": 3, "cohortName": "GI Bleed", "sql": "-- SQL", "json": "{}"}
    ]
    cohort_shared = create_cohort_shared_resource_specifications(cohort_definition_set)
    spec = add_shared_resources(spec, cohort_shared)

    # Step 3: Add modules
    spec = add_module_specifications(
        spec,
        create_cohort_generator_module_specifications(generate_stats=True)
    )
    spec = add_module_specifications(
        spec,
        create_cohort_diagnostics_module_specifications(
            run_inclusion_statistics=True,
            run_incidence_rate=True
        )
    )
    spec = add_module_specifications(
        spec,
        create_characterization_module_specifications(
            target_ids=[1, 2],
            outcome_ids=[3]
        )
    )

    # Step 4: Serialize to JSON
    print(spec.to_json())
`;const H=Object.assign({"./pyqe/__init__.py":u,"./pyqe/api/__init__.py":f,"./pyqe/api/base.py":m,"./pyqe/api/cohort.py":g,"./pyqe/api/concept_query.py":h,"./pyqe/api/concept_set_query.py":y,"./pyqe/api/datasource.py":v,"./pyqe/api/pa_config.py":b,"./pyqe/api/query.py":C,"./pyqe/api/result.py":T,"./pyqe/api/study.py":S,"./pyqe/azure/__init__.py":N,"./pyqe/azure/msal_credentials.py":E,"./pyqe/azure/password_grant.py":w,"./pyqe/azure/refresh_token.py":x,"./pyqe/ql/__init__.py":O,"./pyqe/ql/advanced_time_filter.py":D,"./pyqe/ql/attribute.py":A,"./pyqe/ql/config.py":I,"./pyqe/ql/criteria_group.py":q,"./pyqe/ql/date_period.py":P,"./pyqe/ql/filter_card.py":k,"./pyqe/ql/interaction.py":L,"./pyqe/ql/person.py":F,"./pyqe/settings.yaml":M,"./pyqe/setup.py":R,"./pyqe/shared/__init__.py":j,"./pyqe/shared/b64encode_query.py":z,"./pyqe/shared/decorator.py":U,"./pyqe/shared/settings.py":G,"./pyqe/types/__init__.py":V,"./pyqe/types/enum_types.py":Q,"./pyqe/types/types.py":B,"./pyqe/utils/__init__.py":Y});let n=null,p=!1,c=null;function s(r){self.postMessage(r)}async function K(r,i,o){if(!p){s({type:"status",id:"",data:{state:"connecting"}});try{if(n=await(await import("./pyodide-B3WfjYbf.js")).loadPyodide({indexURL:r||"https://cdn.jsdelivr.net/pyodide/v0.29.0/full/",stdout:e=>{c&&s({type:"stdout",id:c,data:e})},stderr:e=>{c&&s({type:"stderr",id:c,data:e})}}),i&&i.length>0){await n.loadPackagesFromImports("import micropip");const e=n.pyimport("micropip");for(const t of i)try{await e.install(t)}catch(_){console.warn("Failed to install %s:",t,_)}}n.runPython(`
def _capture_open_figures():
    try:
        import matplotlib.pyplot as plt
        import base64, io
    except ImportError:
        return []
    figs = [plt.figure(n) for n in plt.get_fignums()]
    results = []
    for fig in figs:
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
        buf.seek(0)
        results.append(base64.b64encode(buf.read()).decode('utf-8'))
    plt.close('all')
    return results
`);try{n.runPython(W)}catch(e){console.warn("Failed to load Strategus spec builder:",e)}try{for(const[e,t]of Object.entries(H)){const _="/home/pyodide/"+e.replace("./",""),l=_.substring(0,_.lastIndexOf("/"));n.globals.set("__mkdir_path__",l),n.runPython(`
import os
os.makedirs(__mkdir_path__, exist_ok=True)
`),n.globals.delete("__mkdir_path__"),n.FS.writeFile(_,t)}n.runPython(`
import sys
if "/home/pyodide" not in sys.path:
    sys.path.insert(0, "/home/pyodide")
`)}catch(e){console.warn("Failed to load pyqe package:",e)}try{await n.loadPackage("micropip"),await n.runPythonAsync(`
import micropip
_pyqe_deps = ['requests', 'pyyaml', 'six', 'PyJWT', 'python-dotenv']
for _dep in _pyqe_deps:
    try:
        await micropip.install(_dep)
    except Exception:
        pass
del _pyqe_deps, _dep
`)}catch(e){console.warn("Failed to pre-install pyqe dependencies:",e)}if(o&&Object.keys(o).length>0)try{n.globals.set("__env_vars__",n.toPy(o)),n.runPython(`
import os
for _k, _v in __env_vars__.items():
    os.environ[_k] = _v
del _k, _v
`),n.globals.delete("__env_vars__")}catch(e){console.warn("Failed to set environment variables:",e)}p=!0,s({type:"ready",id:""}),s({type:"status",id:"",data:{state:"idle"}})}catch(a){s({type:"error",id:"",data:{ename:"InitializationError",evalue:a instanceof Error?a.message:String(a),traceback:[]}}),s({type:"status",id:"",data:{state:"error"}})}}}function J(r){const i=r.match(/No module named '([^'.]+)'/);return i?i[1]:null}const X={jwt:"PyJWT",yaml:"pyyaml",dotenv:"python-dotenv",cv2:"opencv-python",PIL:"Pillow",sklearn:"scikit-learn",bs4:"beautifulsoup4",attr:"attrs",msal:"msal"};async function $(r,i){if(!n||!p){s({type:"error",id:r,data:{ename:"NotInitializedError",evalue:"Pyodide is not initialized",traceback:[]}});return}c=r,s({type:"status",id:r,data:{state:"busy"}});try{try{n.globals.set("__user_code__",i);const e=n.runPython("from pyodide.code import find_imports as _fi; list(_fi(__user_code__))"),t=e.toJs();e.destroy(),n.globals.delete("__user_code__"),t.length>0&&await n.loadPackage(t)}catch{await n.loadPackagesFromImports(i)}try{n.runPython(`
try:
    import matplotlib
    matplotlib.use('agg')
except ImportError:
    pass
`)}catch{}let o;const a=5;for(let e=0;;e++)try{o=await n.runPythonAsync(i);break}catch(t){const _=t instanceof Error?t.message:String(t),l=J(_);if(!l||e>=a)throw t;const d=X[l]||l;s({type:"stderr",id:r,data:`Installing ${d}...`});try{await n.loadPackage(d)}catch{try{n.globals.set("__pkg_name__",d),await n.runPythonAsync("import micropip; await micropip.install(__pkg_name__)"),n.globals.delete("__pkg_name__")}catch{throw t}}}if(o!=null){const e=String(o);if(!e.includes("matplotlib.figure.Figure")){let t=e;try{t=n.runPython(`repr(${i.split(`
`).pop()})`)||e}catch{}s({type:"result",id:r,data:{"text/plain":t}})}}try{const e=await n.runPythonAsync("_capture_open_figures()"),t=e.toJs();for(const _ of t)s({type:"display_data",id:r,data:{"image/png":_}});e.destroy()}catch{}s({type:"status",id:r,data:{state:"idle"}})}catch(o){const a=o instanceof Error?o.message:String(o),e=o instanceof Error&&o.stack?o.stack.split(`
`):[];s({type:"error",id:r,data:{ename:o instanceof Error?o.constructor.name:"Error",evalue:a,traceback:e}}),s({type:"status",id:r,data:{state:"idle"}})}finally{c=null}}self.onmessage=async r=>{const{type:i,id:o,code:a,indexUrl:e,preloadPackages:t,envVars:_}=r.data;switch(i){case"init":await K(e,t,_);break;case"execute":a&&await $(o,a);break}};
