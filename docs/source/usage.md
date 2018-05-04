# ESGF CWT WPS

### Open [aims2.llnl.gov](http://aims2.llnl.gov/wps/home) in a browser

![home](/docs/source/images/home.png)

### Login using OpenID

Click the login button in the upper right hand corner of the home page. Here
you can login using OpenID, the same credentials as you would use on the CoG
site. If you do not already have an account on CoG, you can create one [here](https://esgf-node.llnl.gov/user/add/?next=https://esgf-node.llnl.gov/projects/esgf-llnl/)

After a successful login you will be redirected to the user profile.

![openid](/docs/source/images/openid.png)

### Setup data credentials using [OAuth2](#oauth2) or [MyProxyClient](#myproxyclient) 

Next you will need to setup your data credentials using either [OAuth2](#oauth2) or 
[MyProxyClient](#myproxyclient). OAuth2 is the preferred method.

![user_profile](/docs/source/images/user_profile.png)

#### OAuth2 (Short-Lived Credential Service)

These credentials will be the same used for CoG.

After a successful authorization you will be redirected to the user profile.

![oauth2](/docs/source/images/slcs.png)

#### MyProxyClient

These credentials will be the same used for CoG.

![mpc](/docs/source/images/mpc.png)

### Select dataset

Next you will need to select a dataset. Here is a [sample](https://aims2.llnl.gov/wps/home/configure?dataset_id=cmip5.output1.CMCC.CMCC-CM.decadal2005.mon.atmos.Amon.r1i2p1.v20121008%7Caims3.llnl.gov&index_node=esgf-node.llnl.gov&query=) 
to get started. Otherwise you can search at the demo [CoG](https://aims2.llnl.gov/search/testproject/) on aims2.
Once you've found a dataset click on the compute link, this will redirect you
to the page where you can configure the WPS process.

![cog](/docs/source/images/cog.png)

### Configure the WPS process

Configure the WPS process by selecting the operation, variable, desired target grid if regridding is needed,
and the domain of interest. Once you've configured the process you can submit it for execution. 

![configure](/docs/source/images/configure.png)

### Submit the process

Click the execute button to have it queued for execution.

![execute](/docs/source/images/configure_execute.png)

### Check the status

After submitting the process you can check the job status by click the Jobs link
in the upper right hand corner.

![status](/docs/source/images/status.png)
