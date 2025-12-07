\# Purview Blob Layout (V1)



Storage account: `stduitcampaigns`

Container: `purview-assets`



Structure:



purview-assets/

├── lenders/

│     ├── {lender\_id}/

│     │        hero.jpg

│     │        carousel\_1.jpg

│     │        carousel\_2.jpg

│     │        ...

│     ├── DMI/

│     ├── Bajaj\_market/

│     ├── Poonawalla STPL/

│     └── ...

└── defaults/

&nbsp;       hero.jpg

&nbsp;       carousel\_1.jpg

&nbsp;       carousel\_2.jpg



Lookup priority:



1\. Token-level og\_image\_url from dbo.redirect\_previews

2\. DEFAULT\_OG\_IMAGE\_URL (points to defaults/hero.jpg)



