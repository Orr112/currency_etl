"""
Customized Exceptions
"""

class WrongFormatException(Exception):
    """
    WrongFormatExecption class

    Exeception raised when the an unsupported format is provided 
    """

class WrongMetaFileException(Exception):
    """
    WrongMetaFileException class

    Exception that can be raised when the meta file format is incorrect.
    
    """