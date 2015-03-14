WBR-cubes
=========

Daily ETL for international and Needish WBR.

Installation
-------------
Clone this repo to your local machine

    git clone git@github.domaindev.com:da/WBR-cubes.git

Install required ruby gems

    sudo bundle install

Compile the reporting jar, while in the reporting directory

    ant

Execute the reporting jar file
    
    java -jar dist/WBR-cubes.jar [environment] [source] [start date (inclusive)] [end date (exclusive)]

**environments** currently supported:
* development (default)
* staging
* production

**sources** currectly supported:
* INTL (default)
* NA
* Needish

How to Deploy
-------------
### Master Branch

    cap [environment] deploy

### Any Branch

    cap [environment] deploy -s branch=[branch name]

**environments** currently supported:
* production
* staging
