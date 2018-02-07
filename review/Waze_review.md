---
title: Waze Annotated Bibliography
author: Dan Flynn
date: February 2018
output:
  pdf_document: default
  html_document: default
bibliography: Waze_review.bib
csl: nature.csl
---


# Table of Contents
1. [Machine learning approaches in transportation safety](#machine)
2. [Crowdsourced data analysis approaches](#crowd)
3. [Spatial regression for road safety](#spatial)

Annotated bibliography of tools and approaches used in existing analyses of road safety data, relevant for the Safety Data Initiative Waze/EDT pilot project.

## Machine learning approaches in transportation safety <a name = "machine"></a>

**@Abbas:2016 Modeling the dynamics of driver's dilemma zone perception using agent based modeling techniques**

- In a driving simulation study, used Agent Based Models (ABM) to investigate how drivers impacts of driving in a 'dilemma zone', such as too close to an intersetion to safely stop. Models include the MATsim dyanamic agent-based traffic simulation model. 

**@Bahouth:2012 Influence of injury risk thresholds on the performance of an algorithm to predict crashes with serious injuries**

- Optimization for first responders, using logistic regression on National Automotive Sampling System / Crashworthiness Data System (NASS/CDS) to determine how variation in injury risk thresholds affects crash predictions. Standard logistic regression approach, where outcomes are binary for each type of crash (separate models, not ordinal).

**@Chiou:2013a A two-stage mining framework to explore key risk conditions on one-vehicle crash severity**

- This research combines data mining and a logistic regressino approach to identify crash severity in one-vechicle crashes. Genetic mining rule (GMR) model developed, to identify 'rules' which correspond to variables most associated with risk of a crash. The variables were then used in a hierarchical logistic regression (mixed logit model) to identify road conditions associated with serious crashes. 
- Similar to proposed SDI Waze project approach, where random forests used to identify combinations of variables highly associated with EDT-level crashes, and then logistic regression used to assign probability of a crash to Waze events and test statistical significance. Use a training/validation approach for the rule-mining, 70% of data for training, 30% for validation.

**@Das:2015 Estimating likelihood of future crashes for crash-prone drivers**

**@Delen:2017 Investigating injury severity risk factors in automobile crashes with predictive analytics and sensitivity analysis methods**

**@Gkritza:2013 Empirical Bayes approach for estimating urban deer-vehicle crashes using police and maintenance records**

**@Gonzalez-Velez:2017 Development of a Prediction Model for Crash Occurrence by Analyzing Traffic Crash and Citation Data**

**@Kwon:2015 Application of classification algorithms for analysis of road safety risk factor dependencies**

**@Lin:2015 Data science application in intelligent transportation systems: An integrative approach for border delay prediction and traffic accident analysis**

**@Lord:2004 Estimating the safety performance of urban road transportation networks**

**@Lord:2005 Poisson, Poisson-gamma and zero-inflated regression models of motor vehicle crashes: balancing statistical fit and theory**

**@Lord:2010 The statistical analysis of crash-frequency data: a review and assessment of methodological alternatives**

**@Morgan:2013 Performance Measures for Prioritizing Highway Safety Improvements Based on Predicted Crash Frequency and Severity**

**@Pal:2016 Factors influencing specificity and sensitivity of injury severity prediction (ISP) algorithm for AACN**

**@Pande:2012 Proactive Assessment of Accident Risk to Improve Safety on a System of Freeways**

**@Saha:2015 Prioritizing Highway Safety Manual’s crash prediction variables using boosted regression trees**

**@Saleem:2016 An Exploratory Computational Piecewise Approach to Characterizing and Analyzing Traffic Accident Data**

**@Shawky:2016 Risk Factors Analysis for Drivers with Multiple Crashes**

- Identifying high-risk drivers using demographic charascteristic, historical violations, and specific violation types with negative binomial regression. Crash estimation model identifies the set of predictors most strongly associated with high-risk drivers. Standard regression approach, models evaluated by AIC.

**@Srinivasan:2015 Crash Prediction Method for Freeway Facilities with High Occupancy Vehicle (HOV) and High Occupancy Toll (HOT) Lanes**

**@Sun:2016 Developing Crash Models with Supporting Vector Machine for Urban Transportation Planning**

**@Vasudevan:2016 Predicting Traffic Flow Regimes From Simulated Connected Vehicle Messages Using Data Analytics and Machine Learning**

**@Wang:2016 Exploration of Advances in Statistical Methodologies for Crash Count and Severity Prediction Models**

**@Wei:2017 Analyzing Traffic Crash Severity in Work Zones under Different Light Conditions**

**@Xie:2007 Predicting motor vehicle collisions using Bayesian neural network models: An empirical analysis**

**@Xu:2014 Modeling crash and fatality counts along mainlines and frontage roads across Texas: The roles of design, the built environment, and weather**

## Crowdsourced data analysis approaches <a name = "crowd"></a>

**@Masino:2017 Learning from the crowd: Road infrastructure monitoring system**

**@Vasudevan:2016 Predicting Traffic Flow Regimes From Simulated Connected Vehicle Messages Using Data Analytics and Machine Learning**
 
## Spatial regression for road safety <a name = "spatial"></a>

**@Gill:2017 Comparison of adjacency and distance-based approaches for spatial analysis of multimodal traffic crash data**

**@Rhee:2016 Spatial regression analysis of traffic crashes in Seoul**

**@Schultz:2015 Use of Roadway Attributes in Hot Spot Identification and Analysis**

**@Zeng:2014 Bayesian spatial joint modeling of traffic crashes on an urban road network**

# References
<!-- Will be auto-populated by bibtex references -->