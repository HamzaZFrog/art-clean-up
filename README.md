Python Script identifying old artifacts managed with JFrog Artifactory, based on their creation date and taking into account an exclusion list.
Parameters : 
- artifactory-url : JFrog Artifactory URL (without the /artifactory suffix)
- older-than : artifacts age (could be days > 6d, months > 6m or years > 6y)
- exclusions-file : json file listing files to excludes based on wildcard patterns
- aql-spec : JFrog Artifactory filespec with the clean up query
- dry-run : allows running the script in dry run mode
- access-token : JFrog Artifactory bearer token
- threads : number of workers

  Pre-requisites :
  - Python 3.9 or later (ideally Python 3.11+)
  - JFrog CLI version 2.77.0 (could also work with older versions)

Command : 
python3 clean_old_artifacts_parallel.py --artifactory-url https://servername.jfrog.io --older-than 6mo --exclusions-file exclusions.json --aql-spec aql-filespec.json --dry-run --access-token referenceToken --threads 10
