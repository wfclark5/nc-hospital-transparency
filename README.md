# North Carolina's Hosptials Services and Standard Charges

### Description
The CMS Price Transparency Initiative created new price transparency requirements that were signed into law under section 2718(e) of the Public 
Health Service Act. The requirements state that each hospital must now post the standard charges and negotiated rates for their provided services in a machine readble format on a public domain. These law applies to both small community hospitals and network of hospitals within a large health system.  


### Problem
Unlike NIH data submissions these data are not required to be submitted to a centralized location in a standard format or structure. For an example of complexity, I will explain the method UNC Health used to share their data. At UNC Health on their price transparency webpage, they have posted [links](https://www.unchealthcare.org/patients-families-visitors/billing-financial-assistance/chargemaster/), to third party web portals that are guarded by ReCaptcha for each of their hosptials. Only when a user scrolls to the bottom of these portals they are shown fine text stating how to click the correct button to download the data. This process would then have to be repeated ten times for each of their hosptial systems to gain insights into their standard charges for their services. Other examples of complexity can be seen in the variety of formats and structure of the data for each hospital. For example the raw formats can be in JSON, CSV, or XLSX (or really anything XML) and the data structures might be multiindex, single indexed, pivoted by Payor or by Service. The lack of standards on how these data are provided to end users leaves room for  hospitals and health systems to purposefully make it hard for citizens to analyze their financials.   

### Solution 

This work solves the transparency problem to query and aggregate all of North Carolina's hospital data assets from their base websites listed on the [North Carolina Hosptial Association](https://www.ncha.org/hospital-price-transparency/). Then once those data have heen queries to place structure and formatting around each of them. Finally once the formatting has been put in place to  to create a single view into the financials of all hospital systems in North Carolina. This singular view is updated on a monthly basis with any data changes that found and located in a single csv file for users to download.  

### Metadata

These data consist of the gross hospital charges, uninsured charge, the max negotatied charge in dollars for fpir main payers in the state of NC (CIGNA, AETNA, BCBSNC, HUMANA, United, MEDCOST, TRICARE: Commercial and MEDICARE) by the services CPT or MS-DRG and service description. Additional metadata for all each the raw, curated, and presentation data assets can be found here: [metadata](https://github.com/wfclark5/nc-hospital-transparency/tree/main/data)    

Example Table: 

|   CPT/MS-DRG   | Procedure Description | Gross Charge | System        | AETNA        | AETNA MEDICARE| BCBS         | BCBS MEDICARE|
| :------------- | :----------:          | -----------: | :------------ | :----------: | -----------:  | -----------: | -----------: | 
|  62269 | HB-BIOPSY OF SPINAL CORD      | 563          | Vidant        | 422.25       | 1392.372303   | 427.88       |1392.37230    |




