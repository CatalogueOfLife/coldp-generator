# ColDP Archive Generator

Conversion tools to create [ColDP archives](https://github.com/CatalogueOfLife/coldp/) from various online sources not readily available otherwise.
The conversion is fully automated so it can run in a scheduler.

To build the jar (once) and then generate a WCVP archive with it:

```
mvn package -pl coldp-generator -am 
java -jar coldp-generator/target/coldp-generator-1.0-SNAPSHOT.jar -s wcvp
```

Currently the project supports ColDP generators for the following sources:

 - [Birdlife](http://datazone.birdlife.org/species/taxonomy): Handbook of the Birds of the World and BirdLife International Digital Checklist of the Birds of the World
 - [ICTV](https://talk.ictvonline.org/taxonomy/w/ictv-taxonomy): ICTV Master Species List (MSL)
 - [IPNI](https://www.ipni.org): The International Plant Names Index (IPNI)
 - [LPSN](https://lpsn.dsmz.de/): List of Prokaryotic names with Standing in Nomenclature
 - [OTL](https://tree.opentreeoflife.org/about/synthesis-release): Open Tree of Life Synthesis Tree (OTL)
 - [OTT](https://tree.opentreeoflife.org/about/taxonomy-version): Open Tree of Life Reference Taxonomy (OTT)
 - [WSC](https://wsc.nmbe.ch/): The World Spider Catalog (WSC)