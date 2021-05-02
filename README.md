# COVIDBedsIndiaDataService

In order to run selenium you'll need a webdriver. For the current implementation get the geckodriver to support firefox. Instructions for geckodriver installation can be found here - https://www.geeksforgeeks.org/how-to-install-selenium-in-python/

While using the docker run command specify  --shm-size 256m. Needed for some seleium based scripts

## Test mode

python3 main.py --mode test