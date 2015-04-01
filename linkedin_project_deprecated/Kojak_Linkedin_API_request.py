"""Get requests from Linkedin API"""

from linkedin import linkedin
import Linkedin_credentials as API_codes


if __name__ == '__main__':
    CONSUMER_KEY = API_codes.CONSUMER_KEY
    CONSUMER_SECRET = API_codes.CONSUMER_SECRET
    USER_TOKEN = API_codes.USER_TOKEN
    USER_SECRET = API_codes.USER_SECRET
    RETURN_URL = 'http://emmanuelesalvati.github.io/'

    authentication = linkedin.\
        LinkedInDeveloperAuthentication(CONSUMER_KEY, CONSUMER_SECRET,
                                        USER_TOKEN, USER_SECRET,
                                        RETURN_URL,
                                        linkedin.PERMISSIONS.enums.values())

    application = linkedin.LinkedInApplication(authentication)
    application.get_profile(selectors=[educations])



# u'degree': u'Doctor of Philosophy (Ph.D.)',
#     u'endDate': {u'year': 2010},
#     u'fieldOfStudy': u'Physics',
#     u'id': 1150055,
#     u'schoolName': u'University of Massachusetts, Amherst',
#     u'startDate': {u'year': 2004}},