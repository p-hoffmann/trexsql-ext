var f=`"""
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
`,u=`"""
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
`,y=`import logging
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
`,h=`import os
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
`,T=`import logging
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
`,E=`import logging
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
`,C=`import json
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
        `,q=`"""
About
-------------
\`pyqe.azure\` submodule of \`pyqe\` contains all azure classes
"""
`,S=`import msal


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
`,A=`import sys
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
`,w=`from pyqe.azure.msal_credentials import _MsalCredentials


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
`,N=`"""
About
-------------
\`pyqe.ql\` submodule of \`pyqe\` contains all query language class definitions

"""
`,I=`import logging
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
`,D=`import logging
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
`,O=`import os
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
        return self._interactions[name]`,x=`import logging
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
`,L=`import logging
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
`,R=`import logging
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
`,k=`feature-flags:
  azure-identity: true
`,j=`import os
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
`,M=`"""
About
-------------
\`pyqe.shared\` submodule of \`pyqe\` contains shared functions

"""
`,U=`import json
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
`,Q=`import functools
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
`,Y=`import pkgutil
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
`,z=`import enum


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
`,G=`from dataclasses import dataclass
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
`,B=`"""
About
-------------
\`pyqe.utils\` submodule of \`pyqe\` contains utility functions and constants

"""
`,H=`"""
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
`;const J=Object.assign({"./pyqe/__init__.py":f,"./pyqe/api/__init__.py":u,"./pyqe/api/base.py":m,"./pyqe/api/cohort.py":g,"./pyqe/api/concept_query.py":y,"./pyqe/api/concept_set_query.py":h,"./pyqe/api/datasource.py":v,"./pyqe/api/pa_config.py":b,"./pyqe/api/query.py":T,"./pyqe/api/result.py":E,"./pyqe/api/study.py":C,"./pyqe/azure/__init__.py":q,"./pyqe/azure/msal_credentials.py":S,"./pyqe/azure/password_grant.py":A,"./pyqe/azure/refresh_token.py":w,"./pyqe/ql/__init__.py":N,"./pyqe/ql/advanced_time_filter.py":I,"./pyqe/ql/attribute.py":D,"./pyqe/ql/config.py":O,"./pyqe/ql/criteria_group.py":x,"./pyqe/ql/date_period.py":P,"./pyqe/ql/filter_card.py":L,"./pyqe/ql/interaction.py":R,"./pyqe/ql/person.py":F,"./pyqe/settings.yaml":k,"./pyqe/setup.py":j,"./pyqe/shared/__init__.py":M,"./pyqe/shared/b64encode_query.py":U,"./pyqe/shared/decorator.py":Q,"./pyqe/shared/settings.py":Y,"./pyqe/types/__init__.py":V,"./pyqe/types/enum_types.py":z,"./pyqe/types/types.py":G,"./pyqe/utils/__init__.py":B});let n=null,p=!1,c=null;function i(s){self.postMessage(s)}async function W(s,r,o){if(!p){i({type:"status",id:"",data:{state:"connecting"}});try{if(n=await(await import("./pyodide-B3WfjYbf.js")).loadPyodide({indexURL:s||"https://cdn.jsdelivr.net/pyodide/v0.29.0/full/",stdout:e=>{c&&i({type:"stdout",id:c,data:e})},stderr:e=>{c&&i({type:"stderr",id:c,data:e})}}),r&&r.length>0){await n.loadPackagesFromImports("import micropip");const e=n.pyimport("micropip");for(const t of r)try{await e.install(t)}catch(a){console.warn(`Failed to install ${t}:`,a)}}n.runPython(`
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
`);try{n.runPython(H)}catch(e){console.warn("Failed to load Strategus spec builder:",e)}try{for(const[e,t]of Object.entries(J)){const a="/home/pyodide/"+e.replace("./",""),l=a.substring(0,a.lastIndexOf("/"));n.runPython(`
import os
os.makedirs("${l}", exist_ok=True)
`),n.FS.writeFile(a,t)}n.runPython(`
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
`)}catch(e){console.warn("Failed to pre-install pyqe dependencies:",e)}if(o&&Object.keys(o).length>0)try{const e=Object.entries(o).map(([t,a])=>`os.environ['${t}'] = '''${a}'''`).join(`
`);n.runPython(`import os
${e}`)}catch(e){console.warn("Failed to set environment variables:",e)}p=!0,i({type:"ready",id:""}),i({type:"status",id:"",data:{state:"idle"}})}catch(_){i({type:"error",id:"",data:{ename:"InitializationError",evalue:_ instanceof Error?_.message:String(_),traceback:[]}}),i({type:"status",id:"",data:{state:"error"}})}}}function K(s){const r=s.match(/No module named '([^'.]+)'/);return r?r[1]:null}const $={jwt:"PyJWT",yaml:"pyyaml",dotenv:"python-dotenv",cv2:"opencv-python",PIL:"Pillow",sklearn:"scikit-learn",bs4:"beautifulsoup4",attr:"attrs",msal:"msal"};async function X(s,r){if(!n||!p){i({type:"error",id:s,data:{ename:"NotInitializedError",evalue:"Pyodide is not initialized",traceback:[]}});return}c=s,i({type:"status",id:s,data:{state:"busy"}});try{try{n.globals.set("__user_code__",r);const e=n.runPython("from pyodide.code import find_imports as _fi; list(_fi(__user_code__))"),t=e.toJs();e.destroy(),n.globals.delete("__user_code__"),t.length>0&&await n.loadPackage(t)}catch{await n.loadPackagesFromImports(r)}try{n.runPython(`
try:
    import matplotlib
    matplotlib.use('agg')
except ImportError:
    pass
`)}catch{}let o;const _=5;for(let e=0;;e++)try{o=await n.runPythonAsync(r);break}catch(t){const a=t instanceof Error?t.message:String(t),l=K(a);if(!l||e>=_)throw t;const d=$[l]||l;i({type:"stderr",id:s,data:`Installing ${d}...`});try{await n.loadPackage(d)}catch{try{await n.runPythonAsync(`import micropip; await micropip.install("${d}")`)}catch{throw t}}}if(o!=null){const e=String(o);if(!e.includes("matplotlib.figure.Figure")){let t=e;try{t=n.runPython(`repr(${r.split(`
`).pop()})`)||e}catch{}i({type:"result",id:s,data:{"text/plain":t}})}}try{const e=await n.runPythonAsync("_capture_open_figures()"),t=e.toJs();for(const a of t)i({type:"display_data",id:s,data:{"image/png":a}});e.destroy()}catch{}i({type:"status",id:s,data:{state:"idle"}})}catch(o){const _=o instanceof Error?o.message:String(o),e=o instanceof Error&&o.stack?o.stack.split(`
`):[];i({type:"error",id:s,data:{ename:o instanceof Error?o.constructor.name:"Error",evalue:_,traceback:e}}),i({type:"status",id:s,data:{state:"idle"}})}finally{c=null}}self.onmessage=async s=>{const{type:r,id:o,code:_,indexUrl:e,preloadPackages:t,envVars:a}=s.data;switch(r){case"init":await W(e,t,a);break;case"execute":_&&await X(o,_);break}};
