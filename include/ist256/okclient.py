from client.api.notebook import Notebook
import os as _os

def __client__(assignment_type, force_auth):
    ok = Notebook(assignment_type)
    if force_auth:
        ok.auth(force=True)
    elif not _os.path.exists(_os.path.join(_os.environ.get("HOME"), ".config/ok/auth_refresh")):
        ok.auth(force=True)
    else:
        ok.auth(inline=True)
        
    return ok
    

def Homework(force_auth=False):
    '''
    Initializes the ok client for Homework submission and returns the ok client.    
    Use force_auth=True to not used cached credentials.
    '''
    assignment_type = 'ok/hw.ok'    
    return __client__(assignment_type, force_auth)

def Lab(force_auth=False):
    '''
    Initializes the ok client for Homework submission and returns the ok client.    
    Use force_auth=True to not used cached credentials.
    '''
    assignment_type = 'ok/lab.ok'    
    return __client__(assignment_type, force_auth)
