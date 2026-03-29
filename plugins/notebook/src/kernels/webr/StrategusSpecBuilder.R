# StrategusSpecBuilder.R
# Standalone Strategus Analysis Specification Builder for WebR
#
# This file provides functions to create Strategus analysis specifications
# without requiring Java dependencies (DatabaseConnector, CohortGenerator).
# Designed for use in WebR (browser-based R) environments.
#
# Usage:
#   source("StrategusSpecBuilder.R")
#   # or in R with Strategus installed:
#   source(system.file("webr/StrategusSpecBuilder.R", package = "Strategus"))
#
# Dependencies:
#   - checkmate (available in WebR via CRAN)
#
# HADES Package Version Tracking (for maintenance):
#   - CohortMethod 5.4.0
#   - CohortDiagnostics 3.3.0
#   - FeatureExtraction 3.7.0
#   - Characterization 2.0.0
#   - Cyclops 3.5.0
#   - SelfControlledCaseSeries (latest)
#   - PatientLevelPrediction (latest)
#   - EvidenceSynthesis (latest)
#   - CohortIncidence (latest)
#   - CohortSurvival (darwin-eu)
#
# Note: Default settings are inlined from the HADES packages listed above.
# If HADES package defaults change, this file may need updates.

# =============================================================================
# Internal Helper Functions
# =============================================================================

# T003: Create module specifications with proper class attributes
.createModuleSpecifications <- function(moduleName, moduleSettings) {
  moduleSpecifications <- list(
    module = moduleName,
    settings = moduleSettings
  )
  class(moduleSpecifications) <- c("ModuleSpecifications", paste0(moduleName, "Specifications"))
  return(moduleSpecifications)
}

# T004: Create shared resources specifications with proper class attributes
.createSharedResourcesSpecifications <- function(className, sharedResourcesSpecifications) {
  class(sharedResourcesSpecifications) <- c(className, "SharedResources")
  return(sharedResourcesSpecifications)
}

# T005: Validate cohort definition set structure (replaces CohortGenerator::isCohortDefinitionSet)
.isCohortDefinitionSet <- function(x) {
  required_cols <- c("cohortId", "cohortName", "sql", "json")
  is.data.frame(x) && all(required_cols %in% names(x))
}

# T006: Convert data frame to list of lists (from CohortGeneratorModule)
.listafy <- function(df) {
  mylist <- list()
  for (i in seq_len(nrow(df))) {
    cohortData <- list(
      cohortId = df$cohortId[i],
      cohortName = df$cohortName[i],
      cohortDefinition = df$json[i]
    )
    mylist[[i]] <- cohortData
  }
  return(mylist)
}

# =============================================================================
# Inlined Default Settings (from HADES packages)
# =============================================================================

# T007: CohortMethod diagnostic thresholds (from CohortMethod::createCmDiagnosticThresholds)
.createDefaultCmDiagnosticThresholds <- function() {
  thresholds <- list(
    mdrrThreshold = 10,
    easeThreshold = 0.25,
    sdmThreshold = 0.1,
    equipoiseThreshold = 0.2,
    generalizabilitySdmThreshold = 1
  )
  class(thresholds) <- "CmDiagnosticThresholds"
  return(thresholds)
}

# T008: Evidence Synthesis diagnostic thresholds (from EvidenceSynthesisModule)
.createDefaultEsDiagnosticThresholds <- function() {
  thresholds <- list(
    mdrrThreshold = 10,
    easeThreshold = 0.25,
    i2Threshold = 0.4,
    tauThreshold = log(2) # ~0.693
  )
  class(thresholds) <- "EsDiagnosticThresholds"
  return(thresholds)
}

# T009: Characterization covariate settings (from FeatureExtraction::createCovariateSettings)
.getDefaultCharacterizationCovariateSettings <- function() {
  settings <- list(
    temporal = FALSE,
    temporalSequence = FALSE,
    # Demographics - all enabled
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useDemographicsAgeGroup = TRUE,
    useDemographicsRace = TRUE,
    useDemographicsEthnicity = TRUE,
    useDemographicsIndexYear = TRUE,
    useDemographicsIndexMonth = TRUE,
    useDemographicsTimeInCohort = TRUE,
    useDemographicsPriorObservationTime = TRUE,
    useDemographicsPostObservationTime = TRUE,
    # Long term covariates
    useConditionGroupEraLongTerm = TRUE,
    useDrugGroupEraOverlapping = TRUE,
    useDrugGroupEraLongTerm = TRUE,
    useProcedureOccurrenceLongTerm = TRUE,
    useMeasurementLongTerm = TRUE,
    useObservationLongTerm = TRUE,
    useDeviceExposureLongTerm = TRUE,
    useVisitConceptCountLongTerm = TRUE,
    # Short term covariates
    useConditionGroupEraShortTerm = TRUE,
    useDrugGroupEraShortTerm = TRUE,
    useProcedureOccurrenceShortTerm = TRUE,
    useMeasurementShortTerm = TRUE,
    useObservationShortTerm = TRUE,
    useDeviceExposureShortTerm = TRUE,
    useVisitConceptCountShortTerm = TRUE,
    # Time windows
    endDays = 0,
    longTermStartDays = -365,
    shortTermStartDays = -30,
    # Concept filtering
    includedCovariateConceptIds = c(),
    excludedCovariateConceptIds = c(),
    includedCovariateIds = c(),
    addDescendantsToInclude = FALSE,
    addDescendantsToExclude = FALSE
  )
  class(settings) <- "covariateSettings"
  attr(settings, "fun") <- "getDbCovariateData"
  return(settings)
}

# T010: Characterization case (during) covariate settings (from Characterization::createDuringCovariateSettings)
.getDefaultCaseCovariateSettings <- function() {
  settings <- list(
    useConditionGroupEraDuring = TRUE,
    useDrugGroupEraDuring = TRUE,
    useProcedureOccurrenceDuring = TRUE,
    useDeviceExposureDuring = TRUE,
    useMeasurementDuring = TRUE,
    useObservationDuring = TRUE,
    useVisitConceptCountDuring = TRUE
  )
  class(settings) <- "covariateSettings"
  attr(settings, "fun") <- "Characterization::getDuringCovariateData"
  return(settings)
}

# T011: CohortDiagnostics temporal covariate settings (from CohortDiagnostics::getDefaultCovariateSettings)
.getDefaultTemporalCovariateSettings <- function() {
  settings <- list(
    temporal = TRUE,
    temporalSequence = FALSE,
    # Condition covariates
    useConditionEraGroupStart = TRUE,
    useConditionEraGroupOverlap = TRUE,
    # Drug covariates
    useDrugEraGroupStart = TRUE,
    useDrugEraGroupOverlap = TRUE,
    # Visit covariates
    useVisitConceptCountStart = TRUE,
    useVisitConceptCountOverlap = TRUE,
    # Time windows (mandatory for CohortDiagnostics)
    temporalStartDays = c(-365, -30, -365, -30, 0, 1, 31, -9999),
    temporalEndDays = c(0, 0, -31, -1, 0, 30, 365, 9999),
    # Concept filtering
    includedCovariateConceptIds = c(),
    excludedCovariateConceptIds = c(),
    includedCovariateIds = c(),
    addDescendantsToInclude = FALSE,
    addDescendantsToExclude = FALSE
  )
  class(settings) <- "covariateSettings"
  attr(settings, "fun") <- "getDbCovariateData"
  return(settings)
}

# =============================================================================
# Cyclops Helper Functions
# =============================================================================

createPrior <- function(priorType = "laplace",
                        variance = 1,
                        exclude = c(),
                        graph = NULL,
                        neighborhood = NULL,
                        useCrossValidation = TRUE,
                        forceIntercept = FALSE) {
  prior <- list(
    priorType = priorType,
    variance = variance,
    exclude = exclude,
    graph = graph,
    neighborhood = neighborhood,
    useCrossValidation = useCrossValidation,
    forceIntercept = forceIntercept
  )
  class(prior) <- "cyclopsPrior"
  return(prior)
}

createControl <- function(maxIterations = 1000,
                          tolerance = 1e-6,
                          convergenceType = "gradient",
                          autoSearch = TRUE,
                          fold = 10,
                          cvRepetitions = 1,
                          startingVariance = 0.01,
                          lowerLimit = 0.01,
                          upperLimit = 20,
                          seed = NULL,
                          resetCoefficients = FALSE,
                          noiseLevel = "silent",
                          threads = 1,
                          cvType = "auto",
                          selectorType = "byPid") {
  control <- list(
    maxIterations = maxIterations,
    tolerance = tolerance,
    convergenceType = convergenceType,
    autoSearch = autoSearch,
    fold = fold,
    cvRepetitions = cvRepetitions,
    startingVariance = startingVariance,
    lowerLimit = lowerLimit,
    upperLimit = upperLimit,
    seed = seed,
    resetCoefficients = resetCoefficients,
    noiseLevel = noiseLevel,
    threads = threads,
    cvType = cvType,
    selectorType = selectorType
  )
  class(control) <- "cyclopsControl"
  return(control)
}

# =============================================================================
# FeatureExtraction Builder Functions
# =============================================================================

createCovariateSettings <- function(useDemographicsGender = FALSE,
                                    useDemographicsAge = FALSE,
                                    useDemographicsAgeGroup = FALSE,
                                    useDemographicsRace = FALSE,
                                    useDemographicsEthnicity = FALSE,
                                    useDemographicsIndexYear = FALSE,
                                    useDemographicsIndexMonth = FALSE,
                                    useDemographicsPriorObservationTime = FALSE,
                                    useDemographicsPostObservationTime = FALSE,
                                    useDemographicsTimeInCohort = FALSE,
                                    useDemographicsIndexYearMonth = FALSE,
                                    useCareSiteId = FALSE,
                                    useConditionOccurrenceAnyTimePrior = FALSE,
                                    useConditionOccurrenceLongTerm = FALSE,
                                    useConditionOccurrenceMediumTerm = FALSE,
                                    useConditionOccurrenceShortTerm = FALSE,
                                    useConditionOccurrencePrimaryInpatientAnyTimePrior = FALSE,
                                    useConditionOccurrencePrimaryInpatientLongTerm = FALSE,
                                    useConditionOccurrencePrimaryInpatientMediumTerm = FALSE,
                                    useConditionOccurrencePrimaryInpatientShortTerm = FALSE,
                                    useConditionEraAnyTimePrior = FALSE,
                                    useConditionEraLongTerm = FALSE,
                                    useConditionEraMediumTerm = FALSE,
                                    useConditionEraShortTerm = FALSE,
                                    useConditionEraOverlapping = FALSE,
                                    useConditionEraStartLongTerm = FALSE,
                                    useConditionEraStartMediumTerm = FALSE,
                                    useConditionEraStartShortTerm = FALSE,
                                    useConditionGroupEraAnyTimePrior = FALSE,
                                    useConditionGroupEraLongTerm = FALSE,
                                    useConditionGroupEraMediumTerm = FALSE,
                                    useConditionGroupEraShortTerm = FALSE,
                                    useConditionGroupEraOverlapping = FALSE,
                                    useConditionGroupEraStartLongTerm = FALSE,
                                    useConditionGroupEraStartMediumTerm = FALSE,
                                    useConditionGroupEraStartShortTerm = FALSE,
                                    useDrugExposureAnyTimePrior = FALSE,
                                    useDrugExposureLongTerm = FALSE,
                                    useDrugExposureMediumTerm = FALSE,
                                    useDrugExposureShortTerm = FALSE,
                                    useDrugEraAnyTimePrior = FALSE,
                                    useDrugEraLongTerm = FALSE,
                                    useDrugEraMediumTerm = FALSE,
                                    useDrugEraShortTerm = FALSE,
                                    useDrugEraOverlapping = FALSE,
                                    useDrugEraStartLongTerm = FALSE,
                                    useDrugEraStartMediumTerm = FALSE,
                                    useDrugEraStartShortTerm = FALSE,
                                    useDrugGroupEraAnyTimePrior = FALSE,
                                    useDrugGroupEraLongTerm = FALSE,
                                    useDrugGroupEraMediumTerm = FALSE,
                                    useDrugGroupEraShortTerm = FALSE,
                                    useDrugGroupEraOverlapping = FALSE,
                                    useDrugGroupEraStartLongTerm = FALSE,
                                    useDrugGroupEraStartMediumTerm = FALSE,
                                    useDrugGroupEraStartShortTerm = FALSE,
                                    useProcedureOccurrenceAnyTimePrior = FALSE,
                                    useProcedureOccurrenceLongTerm = FALSE,
                                    useProcedureOccurrenceMediumTerm = FALSE,
                                    useProcedureOccurrenceShortTerm = FALSE,
                                    useDeviceExposureAnyTimePrior = FALSE,
                                    useDeviceExposureLongTerm = FALSE,
                                    useDeviceExposureMediumTerm = FALSE,
                                    useDeviceExposureShortTerm = FALSE,
                                    useMeasurementAnyTimePrior = FALSE,
                                    useMeasurementLongTerm = FALSE,
                                    useMeasurementMediumTerm = FALSE,
                                    useMeasurementShortTerm = FALSE,
                                    useMeasurementValueAnyTimePrior = FALSE,
                                    useMeasurementValueLongTerm = FALSE,
                                    useMeasurementValueMediumTerm = FALSE,
                                    useMeasurementValueShortTerm = FALSE,
                                    useMeasurementRangeGroupAnyTimePrior = FALSE,
                                    useMeasurementRangeGroupLongTerm = FALSE,
                                    useMeasurementRangeGroupMediumTerm = FALSE,
                                    useMeasurementRangeGroupShortTerm = FALSE,
                                    useMeasurementValueAsConceptAnyTimePrior = FALSE,
                                    useMeasurementValueAsConceptLongTerm = FALSE,
                                    useMeasurementValueAsConceptMediumTerm = FALSE,
                                    useMeasurementValueAsConceptShortTerm = FALSE,
                                    useObservationAnyTimePrior = FALSE,
                                    useObservationLongTerm = FALSE,
                                    useObservationMediumTerm = FALSE,
                                    useObservationShortTerm = FALSE,
                                    useObservationValueAsConceptAnyTimePrior = FALSE,
                                    useObservationValueAsConceptLongTerm = FALSE,
                                    useObservationValueAsConceptMediumTerm = FALSE,
                                    useObservationValueAsConceptShortTerm = FALSE,
                                    useCharlsonIndex = FALSE,
                                    useDcsi = FALSE,
                                    useChads2 = FALSE,
                                    useChads2Vasc = FALSE,
                                    useHfrs = FALSE,
                                    useDistinctConditionCountLongTerm = FALSE,
                                    useDistinctConditionCountMediumTerm = FALSE,
                                    useDistinctConditionCountShortTerm = FALSE,
                                    useDistinctIngredientCountLongTerm = FALSE,
                                    useDistinctIngredientCountMediumTerm = FALSE,
                                    useDistinctIngredientCountShortTerm = FALSE,
                                    useDistinctProcedureCountLongTerm = FALSE,
                                    useDistinctProcedureCountMediumTerm = FALSE,
                                    useDistinctProcedureCountShortTerm = FALSE,
                                    useDistinctMeasurementCountLongTerm = FALSE,
                                    useDistinctMeasurementCountMediumTerm = FALSE,
                                    useDistinctMeasurementCountShortTerm = FALSE,
                                    useDistinctObservationCountLongTerm = FALSE,
                                    useDistinctObservationCountMediumTerm = FALSE,
                                    useDistinctObservationCountShortTerm = FALSE,
                                    useVisitCountLongTerm = FALSE,
                                    useVisitCountMediumTerm = FALSE,
                                    useVisitCountShortTerm = FALSE,
                                    useVisitConceptCountLongTerm = FALSE,
                                    useVisitConceptCountMediumTerm = FALSE,
                                    useVisitConceptCountShortTerm = FALSE,
                                    longTermStartDays = -365,
                                    mediumTermStartDays = -180,
                                    shortTermStartDays = -30,
                                    endDays = 0,
                                    includedCovariateConceptIds = c(),
                                    addDescendantsToInclude = FALSE,
                                    excludedCovariateConceptIds = c(),
                                    addDescendantsToExclude = FALSE,
                                    includedCovariateIds = c()) {
  covariateSettings <- list(
    temporal = FALSE,
    temporalSequence = FALSE
  )
  formalNames <- names(formals(createCovariateSettings))
  anyUseTrue <- FALSE
  for (name in formalNames) {
    value <- get(name)
    if (is.null(value)) {
      value <- vector()
    }
    if (grepl("^use", name)) {
      if (value) {
        covariateSettings[[sub("use", "", name)]] <- value
        anyUseTrue <- TRUE
      }
    } else {
      covariateSettings[[name]] <- value
    }
  }
  if (!anyUseTrue) {
    stop("No covariate analysis selected. Must select at least one")
  }
  attr(covariateSettings, "fun") <- "getDbDefaultCovariateData"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}

createDefaultCovariateSettings <- function(includedCovariateConceptIds = c(),
                                            addDescendantsToInclude = FALSE,
                                            excludedCovariateConceptIds = c(),
                                            addDescendantsToExclude = FALSE,
                                            includedCovariateIds = c()) {
  # Inlined default — covers common demographics + condition/drug/procedure covariates
  settings <- .getDefaultCharacterizationCovariateSettings()
  settings$includedCovariateConceptIds <- includedCovariateConceptIds
  settings$addDescendantsToInclude <- addDescendantsToInclude
  settings$excludedCovariateConceptIds <- excludedCovariateConceptIds
  settings$addDescendantsToExclude <- addDescendantsToExclude
  settings$includedCovariateIds <- includedCovariateIds
  attr(settings, "fun") <- "getDbDefaultCovariateData"
  return(settings)
}

createTemporalCovariateSettings <- function(useDemographicsGender = FALSE,
                                            useDemographicsAge = FALSE,
                                            useDemographicsAgeGroup = FALSE,
                                            useDemographicsRace = FALSE,
                                            useDemographicsEthnicity = FALSE,
                                            useDemographicsIndexYear = FALSE,
                                            useDemographicsIndexMonth = FALSE,
                                            useDemographicsPriorObservationTime = FALSE,
                                            useDemographicsPostObservationTime = FALSE,
                                            useDemographicsTimeInCohort = FALSE,
                                            useDemographicsIndexYearMonth = FALSE,
                                            useConditionEraGroupStart = FALSE,
                                            useConditionEraGroupOverlap = FALSE,
                                            useDrugEraGroupStart = FALSE,
                                            useDrugEraGroupOverlap = FALSE,
                                            useProcedureOccurrenceStart = FALSE,
                                            useProcedureOccurrenceOverlap = FALSE,
                                            useDeviceExposureStart = FALSE,
                                            useDeviceExposureOverlap = FALSE,
                                            useMeasurementStart = FALSE,
                                            useMeasurementOverlap = FALSE,
                                            useObservationStart = FALSE,
                                            useObservationOverlap = FALSE,
                                            useVisitCountStart = FALSE,
                                            useVisitCountOverlap = FALSE,
                                            useVisitConceptCountStart = FALSE,
                                            useVisitConceptCountOverlap = FALSE,
                                            useConditionOccurrenceStart = FALSE,
                                            useConditionOccurrenceOverlap = FALSE,
                                            useDrugExposureStart = FALSE,
                                            useDrugExposureOverlap = FALSE,
                                            useConditionEraStart = FALSE,
                                            useConditionEraOverlap = FALSE,
                                            temporalStartDays = c(-365, -30, 0, 1, 31),
                                            temporalEndDays = c(-31, -1, 0, 30, 365),
                                            includedCovariateConceptIds = c(),
                                            addDescendantsToInclude = FALSE,
                                            excludedCovariateConceptIds = c(),
                                            addDescendantsToExclude = FALSE,
                                            includedCovariateIds = c()) {
  covariateSettings <- list(
    temporal = TRUE,
    temporalSequence = FALSE
  )
  formalNames <- names(formals(createTemporalCovariateSettings))
  anyUseTrue <- FALSE
  for (name in formalNames) {
    value <- get(name)
    if (is.null(value)) value <- vector()
    if (grepl("^use", name)) {
      if (value) {
        covariateSettings[[sub("use", "", name)]] <- value
        anyUseTrue <- TRUE
      }
    } else {
      covariateSettings[[name]] <- value
    }
  }
  if (!anyUseTrue) {
    stop("No covariate analysis selected. Must select at least one")
  }
  attr(covariateSettings, "fun") <- "getDbDefaultCovariateData"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}

createDetailedCovariateSettings <- function(analyses = list()) {
  settings <- list(
    temporal = FALSE,
    temporalSequence = FALSE,
    analyses = analyses
  )
  attr(settings, "fun") <- "getDbCovariateData"
  class(settings) <- "covariateSettings"
  return(settings)
}

createAnalysisDetails <- function(analysisId,
                                  sqlFileName,
                                  parameters = list(),
                                  includedCovariateConceptIds = c(),
                                  addDescendantsToInclude = FALSE,
                                  excludedCovariateConceptIds = c(),
                                  addDescendantsToExclude = FALSE,
                                  includedCovariateIds = c()) {
  details <- list(
    analysisId = analysisId,
    sqlFileName = sqlFileName,
    parameters = parameters,
    includedCovariateConceptIds = includedCovariateConceptIds,
    addDescendantsToInclude = addDescendantsToInclude,
    excludedCovariateConceptIds = excludedCovariateConceptIds,
    addDescendantsToExclude = addDescendantsToExclude,
    includedCovariateIds = includedCovariateIds
  )
  class(details) <- "analysisDetail"
  return(details)
}

# =============================================================================
# CohortMethod Builder Functions
# =============================================================================

createGetDbCohortMethodDataArgs <- function(covariateSettings = createDefaultCovariateSettings(),
                                            removeDuplicateSubjects = "keep first, truncate to second",
                                            firstExposureOnly = TRUE,
                                            washoutPeriod = 365,
                                            nestingCohortId = NULL,
                                            restrictToCommonPeriod = TRUE,
                                            minAge = NULL,
                                            maxAge = NULL,
                                            genderConceptIds = NULL,
                                            studyStartDate = "",
                                            studyEndDate = "",
                                            maxCohortSize = 0) {
  args <- list(
    covariateSettings = covariateSettings,
    removeDuplicateSubjects = removeDuplicateSubjects,
    firstExposureOnly = firstExposureOnly,
    washoutPeriod = washoutPeriod,
    nestingCohortId = nestingCohortId,
    restrictToCommonPeriod = restrictToCommonPeriod,
    minAge = minAge,
    maxAge = maxAge,
    genderConceptIds = genderConceptIds,
    studyStartDate = studyStartDate,
    studyEndDate = studyEndDate,
    maxCohortSize = maxCohortSize
  )
  class(args) <- "GetDbCohortMethodDataArgs"
  return(args)
}

createCreateStudyPopulationArgs <- function(removeSubjectsWithPriorOutcome = TRUE,
                                            priorOutcomeLookback = 99999,
                                            minDaysAtRisk = 1,
                                            maxDaysAtRisk = 99999,
                                            riskWindowStart = 0,
                                            startAnchor = "cohort start",
                                            riskWindowEnd = 0,
                                            endAnchor = "cohort end",
                                            censorAtNewRiskWindow = FALSE) {
  args <- list(
    removeSubjectsWithPriorOutcome = removeSubjectsWithPriorOutcome,
    priorOutcomeLookback = priorOutcomeLookback,
    minDaysAtRisk = minDaysAtRisk,
    maxDaysAtRisk = maxDaysAtRisk,
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    censorAtNewRiskWindow = censorAtNewRiskWindow
  )
  class(args) <- "CreateStudyPopulationArgs"
  return(args)
}

createCreatePsArgs <- function(excludeCovariateIds = c(),
                               includeCovariateIds = c(),
                               maxCohortSizeForFitting = 250000,
                               errorOnHighCorrelation = TRUE,
                               stopOnError = TRUE,
                               prior = createPrior(priorType = "laplace",
                                                   exclude = c(0),
                                                   useCrossValidation = TRUE),
                               control = createControl(noiseLevel = "silent",
                                                       cvType = "auto",
                                                       seed = 1,
                                                       resetCoefficients = TRUE,
                                                       tolerance = 2e-07,
                                                       cvRepetitions = 10,
                                                       startingVariance = 0.01),
                               estimator = "att") {
  args <- list(
    excludeCovariateIds = excludeCovariateIds,
    includeCovariateIds = includeCovariateIds,
    maxCohortSizeForFitting = maxCohortSizeForFitting,
    errorOnHighCorrelation = errorOnHighCorrelation,
    stopOnError = stopOnError,
    prior = prior,
    control = control,
    estimator = estimator
  )
  class(args) <- "CreatePsArgs"
  return(args)
}

createTrimByPsArgs <- function(trimFraction = NULL,
                               equipoiseBounds = NULL,
                               maxWeight = NULL,
                               trimMethod = "symmetric") {
  args <- list(
    trimFraction = trimFraction,
    equipoiseBounds = equipoiseBounds,
    maxWeight = maxWeight,
    trimMethod = trimMethod
  )
  class(args) <- "TrimByPsArgs"
  return(args)
}

createTruncateIptwArgs <- function(maxWeight = 10) {
  args <- list(maxWeight = maxWeight)
  class(args) <- "TruncateIptwArgs"
  return(args)
}

createMatchOnPsArgs <- function(caliper = 0.2,
                                caliperScale = "standardized logit",
                                maxRatio = 1,
                                allowReverseMatch = FALSE,
                                matchColumns = c(),
                                matchCovariateIds = c()) {
  args <- list(
    caliper = caliper,
    caliperScale = caliperScale,
    maxRatio = maxRatio,
    allowReverseMatch = allowReverseMatch,
    matchColumns = matchColumns,
    matchCovariateIds = matchCovariateIds
  )
  class(args) <- "MatchOnPsArgs"
  return(args)
}

createStratifyByPsArgs <- function(numberOfStrata = 10,
                                   baseSelection = "all",
                                   stratificationColumns = c(),
                                   stratificationCovariateIds = c()) {
  args <- list(
    numberOfStrata = numberOfStrata,
    baseSelection = baseSelection,
    stratificationColumns = stratificationColumns,
    stratificationCovariateIds = stratificationCovariateIds
  )
  class(args) <- "StratifyByPsArgs"
  return(args)
}

createComputeCovariateBalanceArgs <- function(subgroupCovariateId = NULL,
                                             maxCohortSize = 250000,
                                             covariateFilter = NULL,
                                             threshold = 0.1,
                                             alpha = 0.05) {
  args <- list(
    subgroupCovariateId = subgroupCovariateId,
    maxCohortSize = maxCohortSize,
    covariateFilter = covariateFilter,
    threshold = threshold,
    alpha = alpha
  )
  class(args) <- "ComputeCovariateBalanceArgs"
  return(args)
}

createFitOutcomeModelArgs <- function(modelType = "cox",
                                     stratified = FALSE,
                                     useCovariates = FALSE,
                                     inversePtWeighting = FALSE,
                                     bootstrapCi = FALSE,
                                     bootstrapReplicates = 200,
                                     interactionCovariateIds = c(),
                                     excludeCovariateIds = c(),
                                     includeCovariateIds = c(),
                                     profileGrid = NULL,
                                     profileBounds = c(log(0.1), log(10)),
                                     prior = createPrior(priorType = "laplace",
                                                         useCrossValidation = TRUE),
                                     control = createControl(cvType = "auto",
                                                             startingVariance = 0.01,
                                                             tolerance = 2e-07,
                                                             noiseLevel = "silent")) {
  args <- list(
    modelType = modelType,
    stratified = stratified,
    useCovariates = useCovariates,
    inversePtWeighting = inversePtWeighting,
    bootstrapCi = bootstrapCi,
    bootstrapReplicates = bootstrapReplicates,
    interactionCovariateIds = interactionCovariateIds,
    excludeCovariateIds = excludeCovariateIds,
    includeCovariateIds = includeCovariateIds,
    profileGrid = profileGrid,
    profileBounds = profileBounds,
    prior = prior,
    control = control
  )
  class(args) <- "FitOutcomeModelArgs"
  return(args)
}

createCmAnalysis <- function(analysisId = 1,
                             description = "",
                             getDbCohortMethodDataArgs,
                             createStudyPopArgs,
                             createPsArgs = NULL,
                             trimByPsArgs = NULL,
                             truncateIptwArgs = NULL,
                             matchOnPsArgs = NULL,
                             stratifyByPsArgs = NULL,
                             computeSharedCovariateBalanceArgs = NULL,
                             computeCovariateBalanceArgs = NULL,
                             fitOutcomeModelArgs = NULL) {
  analysis <- list(
    analysisId = analysisId,
    description = description,
    getDbCohortMethodDataArgs = getDbCohortMethodDataArgs,
    createStudyPopArgs = createStudyPopArgs,
    createPsArgs = createPsArgs,
    trimByPsArgs = trimByPsArgs,
    truncateIptwArgs = truncateIptwArgs,
    matchOnPsArgs = matchOnPsArgs,
    stratifyByPsArgs = stratifyByPsArgs,
    computeSharedCovariateBalanceArgs = computeSharedCovariateBalanceArgs,
    computeCovariateBalanceArgs = computeCovariateBalanceArgs,
    fitOutcomeModelArgs = fitOutcomeModelArgs
  )
  class(analysis) <- "CmAnalysis"
  return(analysis)
}

createOutcome <- function(outcomeId,
                          outcomeOfInterest = TRUE,
                          trueEffectSize = NA,
                          priorOutcomeLookback = NULL,
                          riskWindowStart = NULL,
                          startAnchor = NULL,
                          riskWindowEnd = NULL,
                          endAnchor = NULL) {
  outcome <- list(
    outcomeId = outcomeId,
    outcomeOfInterest = outcomeOfInterest,
    trueEffectSize = trueEffectSize,
    priorOutcomeLookback = priorOutcomeLookback,
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor
  )
  class(outcome) <- "Outcome"
  return(outcome)
}

createTargetComparatorOutcomes <- function(targetId,
                                           comparatorId,
                                           outcomes,
                                           nestingCohortId = NULL,
                                           excludedCovariateConceptIds = c(),
                                           includedCovariateConceptIds = c()) {
  tco <- list(
    targetId = targetId,
    comparatorId = comparatorId,
    outcomes = outcomes,
    nestingCohortId = nestingCohortId,
    excludedCovariateConceptIds = excludedCovariateConceptIds,
    includedCovariateConceptIds = includedCovariateConceptIds
  )
  class(tco) <- "TargetComparatorOutcomes"
  return(tco)
}

createCmDiagnosticThresholds <- function(mdrrThreshold = 10,
                                         easeThreshold = 0.25,
                                         sdmThreshold = 0.1,
                                         sdmAlpha = NULL,
                                         equipoiseThreshold = 0.2,
                                         generalizabilitySdmThreshold = 999) {
  thresholds <- list(
    mdrrThreshold = mdrrThreshold,
    easeThreshold = easeThreshold,
    sdmThreshold = sdmThreshold,
    sdmAlpha = sdmAlpha,
    equipoiseThreshold = equipoiseThreshold,
    generalizabilitySdmThreshold = generalizabilitySdmThreshold
  )
  class(thresholds) <- "CmDiagnosticThresholds"
  return(thresholds)
}

# =============================================================================
# SelfControlledCaseSeries Builder Functions
# =============================================================================

createEraCovariateSettings <- function(includeEraIds,
                                       excludeEraIds = NULL,
                                       label = "Covariates",
                                       stratifyById = FALSE,
                                       start = 0,
                                       startAnchor = "era start",
                                       end = 0,
                                       endAnchor = "era end",
                                       firstOccurrenceOnly = FALSE,
                                       allowRegularization = FALSE,
                                       profileLikelihood = FALSE,
                                       exposureOfInterest = FALSE) {
  settings <- list(
    includeEraIds = includeEraIds,
    excludeEraIds = excludeEraIds,
    label = label,
    stratifyById = stratifyById,
    start = start,
    startAnchor = startAnchor,
    end = end,
    endAnchor = endAnchor,
    firstOccurrenceOnly = firstOccurrenceOnly,
    allowRegularization = allowRegularization,
    profileLikelihood = profileLikelihood,
    exposureOfInterest = exposureOfInterest
  )
  class(settings) <- "EraCovariateSettings"
  return(settings)
}

createAgeCovariateSettings <- function(ageKnots = 5,
                                       allowRegularization = FALSE,
                                       computeConfidenceIntervals = FALSE) {
  settings <- list(
    ageKnots = ageKnots,
    allowRegularization = allowRegularization,
    computeConfidenceIntervals = computeConfidenceIntervals
  )
  class(settings) <- "AgeCovariateSettings"
  return(settings)
}

createSeasonalityCovariateSettings <- function(seasonKnots = 5,
                                               allowRegularization = FALSE,
                                               computeConfidenceIntervals = FALSE) {
  settings <- list(
    seasonKnots = seasonKnots,
    allowRegularization = allowRegularization,
    computeConfidenceIntervals = computeConfidenceIntervals
  )
  class(settings) <- "SeasonalityCovariateSettings"
  return(settings)
}

createCalendarTimeCovariateSettings <- function(calendarTimeKnots = 5,
                                                allowRegularization = FALSE,
                                                computeConfidenceIntervals = FALSE) {
  settings <- list(
    calendarTimeKnots = calendarTimeKnots,
    allowRegularization = allowRegularization,
    computeConfidenceIntervals = computeConfidenceIntervals
  )
  class(settings) <- "CalendarTimeCovariateSettings"
  return(settings)
}

createControlIntervalSettings <- function(includeEraIds = NULL,
                                          excludeEraIds = NULL,
                                          start = 0,
                                          startAnchor = "era start",
                                          end = 0,
                                          endAnchor = "era end",
                                          firstOccurrenceOnly = FALSE) {
  settings <- list(
    includeEraIds = includeEraIds,
    excludeEraIds = excludeEraIds,
    start = start,
    startAnchor = startAnchor,
    end = end,
    endAnchor = endAnchor,
    firstOccurrenceOnly = firstOccurrenceOnly
  )
  class(settings) <- "ControlIntervalSettings"
  return(settings)
}

createGetDbSccsDataArgs <- function(nestingCohortId = NULL,
                                    deleteCovariatesSmallCount = 0,
                                    studyStartDates = c(),
                                    studyEndDates = c(),
                                    maxCasesPerOutcome = 0,
                                    exposureIds = "exposureId",
                                    customCovariateIds = NULL) {
  args <- list(
    nestingCohortId = nestingCohortId,
    deleteCovariatesSmallCount = deleteCovariatesSmallCount,
    studyStartDates = studyStartDates,
    studyEndDates = studyEndDates,
    maxCasesPerOutcome = maxCasesPerOutcome,
    exposureIds = exposureIds,
    customCovariateIds = customCovariateIds
  )
  class(args) <- "GetDbSccsDataArgs"
  return(args)
}

# SCCS-specific version (different from CohortMethod's createCreateStudyPopulationArgs)
.SelfControlledCaseSeries_createCreateStudyPopulationArgs <- function(firstOutcomeOnly = FALSE,
                                                                      naivePeriod = 0,
                                                                      minAge = NULL,
                                                                      maxAge = NULL,
                                                                      genderConceptIds = NULL,
                                                                      restrictTimeToEraId = NULL) {
  args <- list(
    firstOutcomeOnly = firstOutcomeOnly,
    naivePeriod = naivePeriod,
    minAge = minAge,
    maxAge = maxAge,
    genderConceptIds = genderConceptIds,
    restrictTimeToEraId = restrictTimeToEraId
  )
  class(args) <- "CreateStudyPopulationArgs"
  return(args)
}

createCreateSccsIntervalDataArgs <- function(eraCovariateSettings,
                                             ageCovariateSettings = NULL,
                                             seasonalityCovariateSettings = NULL,
                                             calendarTimeCovariateSettings = NULL,
                                             minCasesForTimeCovariates = 10000,
                                             endOfObservationEraLength = 30,
                                             eventDependentObservation = FALSE) {
  args <- list(
    eraCovariateSettings = eraCovariateSettings,
    ageCovariateSettings = ageCovariateSettings,
    seasonalityCovariateSettings = seasonalityCovariateSettings,
    calendarTimeCovariateSettings = calendarTimeCovariateSettings,
    minCasesForTimeCovariates = minCasesForTimeCovariates,
    endOfObservationEraLength = endOfObservationEraLength,
    eventDependentObservation = eventDependentObservation
  )
  class(args) <- "CreateSccsIntervalDataArgs"
  return(args)
}

createCreateScriIntervalDataArgs <- function(eraCovariateSettings,
                                             controlIntervalSettings) {
  args <- list(
    eraCovariateSettings = eraCovariateSettings,
    controlIntervalSettings = controlIntervalSettings
  )
  class(args) <- "CreateScriIntervalDataArgs"
  return(args)
}

createFitSccsModelArgs <- function(prior = createPrior("laplace", useCrossValidation = TRUE),
                                   control = createControl(cvType = "auto",
                                                           selectorType = "byPid",
                                                           startingVariance = 0.1,
                                                           seed = 1,
                                                           resetCoefficients = TRUE,
                                                           noiseLevel = "quiet"),
                                   profileGrid = NULL,
                                   profileBounds = c(log(0.1), log(10))) {
  args <- list(
    prior = prior,
    control = control,
    profileGrid = profileGrid,
    profileBounds = profileBounds
  )
  class(args) <- "FitSccsModelArgs"
  return(args)
}

createSccsAnalysis <- function(analysisId = 1,
                               description = "",
                               getDbSccsDataArgs,
                               createStudyPopulationArgs,
                               createIntervalDataArgs,
                               fitSccsModelArgs) {
  analysis <- list(
    analysisId = analysisId,
    description = description,
    getDbSccsDataArgs = getDbSccsDataArgs,
    createStudyPopulationArgs = createStudyPopulationArgs,
    createIntervalDataArgs = createIntervalDataArgs,
    fitSccsModelArgs = fitSccsModelArgs
  )
  class(analysis) <- "SccsAnalysis"
  return(analysis)
}

createExposure <- function(exposureId,
                           exposureIdRef = "exposureId",
                           trueEffectSize = NA) {
  exposure <- list(
    exposureId = exposureId,
    exposureIdRef = exposureIdRef,
    trueEffectSize = trueEffectSize
  )
  class(exposure) <- "Exposure"
  return(exposure)
}

createExposuresOutcome <- function(outcomeId,
                                   exposures,
                                   nestingCohortId = NULL) {
  eo <- list(
    outcomeId = outcomeId,
    exposures = exposures,
    nestingCohortId = nestingCohortId
  )
  class(eo) <- "ExposuresOutcome"
  return(eo)
}

createSccsDiagnosticThresholds <- function(mdrrThreshold = 10,
                                           easeThreshold = 0.25,
                                           timeTrendMaxRatio = 1.1,
                                           rareOutcomeMaxPrevalence = 0.1,
                                           eventObservationDependenceNullBounds = c(0.5, 2.0),
                                           eventExposureDependenceNullBounds = c(0.8, 1.25)) {
  thresholds <- list(
    mdrrThreshold = mdrrThreshold,
    easeThreshold = easeThreshold,
    timeTrendMaxRatio = timeTrendMaxRatio,
    rareOutcomeMaxPrevalence = rareOutcomeMaxPrevalence,
    eventObservationDependenceNullBounds = eventObservationDependenceNullBounds,
    eventExposureDependenceNullBounds = eventExposureDependenceNullBounds
  )
  class(thresholds) <- "SccsDiagnosticThresholds"
  return(thresholds)
}

createSccsAnalysesSpecifications <- function(sccsAnalysisList,
                                             exposuresOutcomeList,
                                             analysesToExclude = NULL,
                                             combineDataFetchAcrossOutcomes = FALSE,
                                             sccsDiagnosticThresholds = createSccsDiagnosticThresholds(),
                                             controlType = "outcome") {
  specs <- list(
    sccsAnalysisList = sccsAnalysisList,
    exposuresOutcomeList = exposuresOutcomeList,
    analysesToExclude = analysesToExclude,
    combineDataFetchAcrossOutcomes = combineDataFetchAcrossOutcomes,
    sccsDiagnosticThresholds = sccsDiagnosticThresholds,
    controlType = controlType
  )
  class(specs) <- "SccsAnalysesSpecifications"
  return(specs)
}

# =============================================================================
# PatientLevelPrediction Builder Functions
# =============================================================================

createStudyPopulationSettings <- function(binary = TRUE,
                                          includeAllOutcomes = TRUE,
                                          firstExposureOnly = FALSE,
                                          washoutPeriod = 0,
                                          removeSubjectsWithPriorOutcome = TRUE,
                                          priorOutcomeLookback = 99999,
                                          requireTimeAtRisk = TRUE,
                                          minTimeAtRisk = 364,
                                          riskWindowStart = 1,
                                          startAnchor = "cohort start",
                                          riskWindowEnd = 365,
                                          endAnchor = "cohort start",
                                          restrictTarToCohortEnd = FALSE) {
  result <- list(
    binary = binary,
    includeAllOutcomes = includeAllOutcomes,
    firstExposureOnly = firstExposureOnly,
    washoutPeriod = washoutPeriod,
    removeSubjectsWithPriorOutcome = removeSubjectsWithPriorOutcome,
    priorOutcomeLookback = priorOutcomeLookback,
    requireTimeAtRisk = requireTimeAtRisk,
    minTimeAtRisk = minTimeAtRisk,
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    restrictTarToCohortEnd = restrictTarToCohortEnd
  )
  class(result) <- "populationSettings"
  return(result)
}

createRestrictPlpDataSettings <- function(studyStartDate = "",
                                          studyEndDate = "",
                                          firstExposureOnly = FALSE,
                                          washoutPeriod = 0,
                                          sampleSize = NULL) {
  result <- list(
    studyStartDate = studyStartDate,
    studyEndDate = studyEndDate,
    firstExposureOnly = firstExposureOnly,
    washoutPeriod = washoutPeriod,
    sampleSize = sampleSize
  )
  class(result) <- "restrictPlpDataSettings"
  return(result)
}

createPreprocessSettings <- function(minFraction = 0.001,
                                     normalize = TRUE,
                                     removeRedundancy = TRUE) {
  result <- list(
    minFraction = minFraction,
    normalize = normalize,
    removeRedundancy = removeRedundancy
  )
  class(result) <- "preprocessSettings"
  return(result)
}

createDefaultSplitSetting <- function(testFraction = 0.25,
                                      trainFraction = 0.75,
                                      splitSeed = sample(100000, 1),
                                      nfold = 3,
                                      type = "stratified") {
  splitSettings <- list(
    test = testFraction,
    train = trainFraction,
    seed = splitSeed,
    nfold = nfold
  )
  if (type == "stratified") attr(splitSettings, "fun") <- "randomSplitter"
  if (type == "time") attr(splitSettings, "fun") <- "timeSplitter"
  if (type == "subject") attr(splitSettings, "fun") <- "subjectSplitter"
  class(splitSettings) <- "splitSettings"
  return(splitSettings)
}

createSampleSettings <- function(type = "none",
                                 numberOutcomestoNonOutcomes = 1,
                                 sampleSeed = sample(10000, 1)) {
  sampleSettings <- list(
    numberOutcomestoNonOutcomes = numberOutcomestoNonOutcomes,
    sampleSeed = ifelse(type == "none", 1, sampleSeed)
  )
  if (type == "none") attr(sampleSettings, "fun") <- "sameData"
  if (type == "underSample") attr(sampleSettings, "fun") <- "underSampleData"
  if (type == "overSample") attr(sampleSettings, "fun") <- "overSampleData"
  class(sampleSettings) <- "sampleSettings"
  return(sampleSettings)
}

createFeatureEngineeringSettings <- function(type = "none") {
  featureEngineeringSettings <- list()
  if (type == "none") attr(featureEngineeringSettings, "fun") <- "sameData"
  class(featureEngineeringSettings) <- "featureEngineeringSettings"
  return(featureEngineeringSettings)
}

createUnivariateFeatureSelection <- function(k = 100) {
  featureEngineeringSettings <- list(k = as.integer(k))
  attr(featureEngineeringSettings, "fun") <- "univariateFeatureSelection"
  class(featureEngineeringSettings) <- "featureEngineeringSettings"
  return(featureEngineeringSettings)
}

createRandomForestFeatureSelection <- function(ntrees = 2000, maxDepth = 17) {
  featureEngineeringSettings <- list(ntrees = ntrees, maxDepth = maxDepth)
  attr(featureEngineeringSettings, "fun") <- "randomForestFeatureSelection"
  class(featureEngineeringSettings) <- "featureEngineeringSettings"
  return(featureEngineeringSettings)
}

createHyperparameterSettings <- function(search = "grid",
                                         tuningMetric = NULL,
                                         sampleSize = NULL,
                                         randomSeed = NULL,
                                         generator = NULL) {
  result <- list(
    search = search,
    tuningMetric = tuningMetric,
    sampleSize = sampleSize,
    randomSeed = randomSeed,
    generator = generator
  )
  class(result) <- "hyperparameterSettings"
  return(result)
}

createCohortCovariateSettings <- function(cohortName,
                                          settingId,
                                          cohortDatabaseSchema = NULL,
                                          cohortTable = NULL,
                                          cohortId,
                                          startDay = -30,
                                          endDay = 0,
                                          count = FALSE,
                                          ageInteraction = FALSE,
                                          lnAgeInteraction = FALSE,
                                          analysisId = 456) {
  covariateSettings <- list(
    covariateName = cohortName,
    covariateId = cohortId * 100000 + settingId * 1000 + analysisId,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohortId,
    startDay = startDay,
    endDays = endDay,
    count = count,
    ageInteraction = ageInteraction,
    lnAgeInteraction = lnAgeInteraction,
    analysisId = analysisId
  )
  attr(covariateSettings, "fun") <- "PatientLevelPrediction::getCohortCovariateData"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}

createModelDesign <- function(targetId = NULL,
                              outcomeId = NULL,
                              restrictPlpDataSettings = createRestrictPlpDataSettings(),
                              populationSettings = createStudyPopulationSettings(),
                              covariateSettings = createDefaultCovariateSettings(),
                              featureEngineeringSettings = NULL,
                              sampleSettings = NULL,
                              preprocessSettings = NULL,
                              modelSettings = NULL,
                              splitSettings = createDefaultSplitSetting(),
                              hyperparameterSettings = createHyperparameterSettings(),
                              runCovariateSummary = TRUE) {
  if (is.null(featureEngineeringSettings)) {
    featureEngineeringSettings <- list(createFeatureEngineeringSettings(type = "none"))
  }
  if (is.null(sampleSettings)) {
    sampleSettings <- list(createSampleSettings(type = "none"))
  }
  if (is.null(preprocessSettings)) {
    preprocessSettings <- createPreprocessSettings(minFraction = 0.001, normalize = TRUE)
  }

  settings <- list(
    targetId = targetId,
    outcomeId = outcomeId,
    restrictPlpDataSettings = restrictPlpDataSettings,
    covariateSettings = covariateSettings,
    populationSettings = populationSettings,
    sampleSettings = sampleSettings,
    featureEngineeringSettings = featureEngineeringSettings,
    preprocessSettings = preprocessSettings,
    modelSettings = modelSettings,
    splitSettings = splitSettings,
    hyperparameterSettings = hyperparameterSettings,
    runCovariateSummary = runCovariateSummary
  )
  class(settings) <- "modelDesign"
  return(settings)
}

createValidationDesign <- function(targetId,
                                   outcomeId,
                                   populationSettings = NULL,
                                   restrictPlpDataSettings = NULL,
                                   plpModelList,
                                   recalibrate = NULL,
                                   runCovariateSummary = TRUE) {
  design <- list(
    targetId = targetId,
    outcomeId = outcomeId,
    populationSettings = populationSettings,
    plpModelList = plpModelList,
    restrictPlpDataSettings = restrictPlpDataSettings,
    recalibrate = recalibrate,
    runCovariateSummary = runCovariateSummary
  )
  class(design) <- "validationDesign"
  return(design)
}

# PLP Model Settings (set* functions)

setLassoLogisticRegression <- function(variance = 0.01,
                                       seed = NULL,
                                       includeCovariateIds = c(),
                                       noShrinkage = c(0),
                                       threads = -1,
                                       forceIntercept = FALSE,
                                       upperLimit = 20,
                                       lowerLimit = 0.01,
                                       tolerance = 2e-06,
                                       maxIterations = 3000,
                                       priorCoefs = NULL) {
  if (is.null(seed)) seed <- as.integer(sample(100000000, 1))
  param <- list(
    priorParams = list(priorType = "laplace", forceIntercept = forceIntercept,
                       variance = variance, exclude = noShrinkage),
    includeCovariateIds = includeCovariateIds,
    upperLimit = upperLimit, lowerLimit = lowerLimit, priorCoefs = priorCoefs
  )
  settings <- list(
    modelName = "lassoLogisticRegression", modelType = "binary",
    cyclopsModelType = "logistic", priorfunction = "Cyclops::createPrior",
    selectorType = "byPid", crossValidationInPrior = TRUE,
    addIntercept = TRUE, useControl = TRUE, seed = seed,
    threads = threads, tolerance = tolerance, cvRepetitions = 1,
    maxIterations = maxIterations, saveType = "RtoJson", predict = "predictCyclops"
  )
  result <- list(fitFunction = "fitCyclopsModel", param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

setRidgeRegression <- function(variance = 0.01,
                                seed = NULL,
                                includeCovariateIds = c(),
                                noShrinkage = c(0),
                                threads = -1,
                                forceIntercept = FALSE,
                                upperLimit = 20,
                                lowerLimit = 0.01,
                                tolerance = 2e-06,
                                maxIterations = 3000,
                                priorCoefs = NULL) {
  if (is.null(seed)) seed <- as.integer(sample(100000000, 1))
  param <- list(
    priorParams = list(priorType = "normal", forceIntercept = forceIntercept,
                       variance = variance, exclude = noShrinkage),
    includeCovariateIds = includeCovariateIds,
    upperLimit = upperLimit, lowerLimit = lowerLimit, priorCoefs = priorCoefs
  )
  settings <- list(
    modelName = "ridgeLogisticRegression", modelType = "binary",
    cyclopsModelType = "logistic", priorfunction = "Cyclops::createPrior",
    selectorType = "byPid", crossValidationInPrior = TRUE,
    addIntercept = TRUE, useControl = TRUE, seed = seed,
    threads = threads, tolerance = tolerance, cvRepetitions = 1,
    maxIterations = maxIterations, saveType = "RtoJson", predict = "predictCyclops"
  )
  result <- list(fitFunction = "fitCyclopsModel", param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

setCoxModel <- function(variance = 0.01,
                         seed = NULL,
                         includeCovariateIds = c(),
                         noShrinkage = c(),
                         threads = -1,
                         upperLimit = 20,
                         lowerLimit = 0.01,
                         tolerance = 2e-07,
                         maxIterations = 3000) {
  if (is.null(seed)) seed <- as.integer(sample(100000000, 1))
  param <- list(
    priorParams = list(priorType = "laplace", variance = variance, exclude = noShrinkage),
    includeCovariateIds = includeCovariateIds,
    upperLimit = upperLimit, lowerLimit = lowerLimit
  )
  settings <- list(
    cyclopsModelType = "cox", modelType = "survival", modelName = "coxLasso",
    priorfunction = "Cyclops::createPrior", selectorType = "byRow",
    crossValidationInPrior = TRUE, addIntercept = FALSE, useControl = TRUE,
    seed = seed, threads = threads, tolerance = tolerance,
    cvRepetitions = 1, maxIterations = maxIterations,
    saveType = "RtoJson", predict = "predictCyclops"
  )
  result <- list(fitFunction = "fitCyclopsModel", param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

setIterativeHardThresholding <- function(K = 10,
                                         penalty = "bic",
                                         seed = sample(100000, 1),
                                         exclude = c(),
                                         forceIntercept = FALSE,
                                         fitBestSubset = FALSE,
                                         initialRidgeVariance = 0.1,
                                         tolerance = 1e-08,
                                         maxIterations = 10000,
                                         threshold = 1e-06,
                                         delta = 0) {
  if (is.null(seed)) seed <- as.integer(sample(100000000, 1))
  param <- list(
    priorParams = list(K = K, penalty = penalty, exclude = exclude,
                       forceIntercept = forceIntercept),
    fitBestSubset = fitBestSubset,
    initialRidgeVariance = initialRidgeVariance,
    tolerance = tolerance, maxIterations = maxIterations,
    threshold = threshold, delta = delta
  )
  settings <- list(
    modelName = "iterativeHardThresholding", modelType = "binary",
    seed = seed, saveType = "RtoJson", predict = "predictCyclops"
  )
  result <- list(fitFunction = "fitCyclopsModel", param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

setGradientBoostingMachine <- function(ntrees = c(100, 300),
                                       nthread = 20,
                                       earlyStopRound = 25,
                                       maxDepth = c(4, 6, 8),
                                       minChildWeight = 1,
                                       learnRate = c(0.05, 0.1, 0.3),
                                       scalePosWeight = 1,
                                       lambda = 1,
                                       alpha = 0,
                                       seed = sample(10000000, 1)) {
  param <- list(
    ntrees = ntrees, nthread = nthread, earlyStopRound = earlyStopRound,
    maxDepth = maxDepth, minChildWeight = minChildWeight,
    learnRate = learnRate, scalePosWeight = scalePosWeight,
    lambda = lambda, alpha = alpha, seed = list(as.integer(seed))
  )
  settings <- list(
    modelType = "binary", seed = seed, modelName = "gradientBoostingMachine",
    saveType = "xgboost", predict = "predictXgboost"
  )
  result <- list(fitFunction = "fitXgboost", param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

setLightGBM <- function(nthread = 20,
                         earlyStopRound = 25,
                         numIterations = c(100),
                         numLeaves = c(31),
                         maxDepth = c(5, 10),
                         minDataInLeaf = c(20),
                         learningRate = c(0.05, 0.1, 0.3),
                         lambdaL1 = c(0),
                         lambdaL2 = c(0),
                         scalePosWeight = 1,
                         isUnbalance = FALSE,
                         seed = sample(10000000, 1)) {
  param <- list(
    nthread = nthread, earlyStopRound = earlyStopRound,
    numIterations = numIterations, numLeaves = numLeaves,
    maxDepth = maxDepth, minDataInLeaf = minDataInLeaf,
    learningRate = learningRate, lambdaL1 = lambdaL1, lambdaL2 = lambdaL2,
    scalePosWeight = scalePosWeight, isUnbalance = isUnbalance,
    seed = list(as.integer(seed))
  )
  settings <- list(
    modelType = "binary", seed = seed, modelName = "lightGBM",
    saveType = "lightgbm", predict = "predictLightGBM"
  )
  result <- list(fitFunction = "fitLightGBM", param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

setAdaBoost <- function(nEstimators = list(10, 50, 200),
                         learningRate = list(1, 0.5, 0.1),
                         seed = sample(1000000, 1)) {
  param <- list(
    nEstimators = nEstimators, learningRate = learningRate,
    seed = list(as.integer(seed))
  )
  settings <- list(
    modelType = "binary", seed = seed, modelName = "adaboost",
    pythonModule = "sklearn.ensemble", pythonClass = "AdaBoostClassifier",
    saveType = "saveLoadSklearn", predict = "predictSklearn"
  )
  result <- list(param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

setDecisionTree <- function(criterion = list("gini"),
                             splitter = list("best"),
                             maxDepth = list(4L, 10L, NULL),
                             minSamplesSplit = list(2, 10),
                             minSamplesLeaf = list(10, 50),
                             minWeightFractionLeaf = list(0),
                             maxFeatures = list(100, "sqrt", NULL),
                             maxLeafNodes = list(NULL),
                             minImpurityDecrease = list(1e-7),
                             classWeight = list(NULL),
                             seed = sample(1000000, 1)) {
  param <- list(
    criterion = criterion, splitter = splitter, maxDepth = maxDepth,
    minSamplesSplit = minSamplesSplit, minSamplesLeaf = minSamplesLeaf,
    minWeightFractionLeaf = minWeightFractionLeaf, maxFeatures = maxFeatures,
    maxLeafNodes = maxLeafNodes, minImpurityDecrease = minImpurityDecrease,
    classWeight = classWeight, seed = list(as.integer(seed))
  )
  settings <- list(
    modelType = "binary", seed = seed, modelName = "decisionTree",
    pythonModule = "sklearn.tree", pythonClass = "DecisionTreeClassifier",
    saveType = "saveLoadSklearn", predict = "predictSklearn"
  )
  result <- list(param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

setMLP <- function(hiddenLayerSizes = list(c(100), c(20)),
                    activation = list("relu"),
                    solver = list("adam"),
                    alpha = list(0.3, 0.01, 0.0001, 0.000001),
                    batchSize = list("auto"),
                    learningRate = list("constant"),
                    learningRateInit = list(0.001),
                    powerT = list(0.5),
                    maxIter = list(200, 100),
                    shuffle = list(TRUE),
                    tol = list(0.0001),
                    warmStart = list(TRUE),
                    momentum = list(0.9),
                    nesterovsMomentum = list(TRUE),
                    earlyStopping = list(FALSE),
                    validationFraction = list(0.1),
                    beta1 = list(0.9),
                    beta2 = list(0.999),
                    epsilon = list(1e-8),
                    nIterNoChange = list(10),
                    seed = sample(100000, 1)) {
  param <- list(
    hiddenLayerSizes = hiddenLayerSizes, activation = activation,
    solver = solver, alpha = alpha, batchSize = batchSize,
    learningRate = learningRate, learningRateInit = learningRateInit,
    powerT = powerT, maxIter = maxIter, shuffle = shuffle,
    tol = tol, warmStart = warmStart, momentum = momentum,
    nesterovsMomentum = nesterovsMomentum, earlyStopping = earlyStopping,
    validationFraction = validationFraction, beta1 = beta1,
    beta2 = beta2, epsilon = epsilon, nIterNoChange = nIterNoChange,
    seed = list(as.integer(seed))
  )
  settings <- list(
    modelType = "binary", seed = seed, modelName = "mlp",
    pythonModule = "sklearn.neural_network", pythonClass = "MLPClassifier",
    saveType = "saveLoadSklearn", predict = "predictSklearn"
  )
  result <- list(param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

setNaiveBayes <- function() {
  param <- list(none = "true")
  settings <- list(
    modelName = "naiveBayes", modelType = "binary", seed = 0L,
    pythonModule = "sklearn.naive_bayes", pythonClass = "GaussianNB",
    saveType = "saveLoadSklearn", predict = "predictSklearn"
  )
  result <- list(param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

setRandomForest <- function(ntrees = list(100, 500),
                             criterion = list("gini"),
                             maxDepth = list(4, 10, 17),
                             minSamplesSplit = list(2, 5),
                             minSamplesLeaf = list(1, 10),
                             minWeightFractionLeaf = list(0),
                             mtries = list("sqrt", "log2"),
                             maxLeafNodes = list(NULL),
                             minImpurityDecrease = list(0),
                             bootstrap = list(TRUE),
                             maxSamples = list(NULL, 0.9),
                             oobScore = list(FALSE),
                             nJobs = list(NULL),
                             classWeight = list(NULL),
                             seed = sample(100000, 1)) {
  param <- list(
    ntrees = ntrees, criterion = criterion, maxDepth = maxDepth,
    minSamplesSplit = minSamplesSplit, minSamplesLeaf = minSamplesLeaf,
    minWeightFractionLeaf = minWeightFractionLeaf, mtries = mtries,
    maxLeafNodes = maxLeafNodes, minImpurityDecrease = minImpurityDecrease,
    bootstrap = bootstrap, maxSamples = maxSamples, oobScore = oobScore,
    nJobs = nJobs, classWeight = classWeight, seed = list(as.integer(seed))
  )
  settings <- list(
    modelType = "binary", seed = seed, modelName = "randomForest",
    pythonModule = "sklearn.ensemble", pythonClass = "RandomForestClassifier",
    saveType = "saveLoadSklearn", predict = "predictSklearn"
  )
  result <- list(param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

setSVM <- function(C = list(1, 0.9, 2, 0.1),
                    kernel = list("rbf"),
                    degree = list(1, 3, 5),
                    gamma = list("scale", 1e-4, 3e-5, 0.001, 0.01, 0.25),
                    coef0 = list(0.0),
                    shrinking = list(TRUE),
                    tol = list(0.001),
                    classWeight = list(NULL),
                    cacheSize = 500,
                    seed = sample(100000, 1)) {
  param <- list(
    C = C, kernel = kernel, degree = degree, gamma = gamma,
    coef0 = coef0, shrinking = shrinking, tol = tol,
    cacheSize = list(cacheSize), classWeight = classWeight,
    seed = list(as.integer(seed))
  )
  settings <- list(
    modelType = "binary", seed = seed, modelName = "svm",
    pythonModule = "sklearn.svm", pythonClass = "SVC",
    saveType = "saveLoadSklearn", predict = "predictSklearn"
  )
  result <- list(param = param, settings = settings)
  class(result) <- "modelSettings"
  return(result)
}

# =============================================================================
# EvidenceSynthesis Builder Functions
# =============================================================================

generateBayesianHMAsettings <- function(primaryEffectPriorStd = 1.0,
                                        secondaryEffectPriorStd = 1.0,
                                        globalExposureEffectPriorMean = c(0.0),
                                        globalExposureEffectPriorStd = c(2.0),
                                        primaryEffectPrecisionPrior = c(1.0, 1.0),
                                        secondaryEffectPrecisionPrior = c(1.0, 1.0),
                                        errorPrecisionPrior = c(1.0, 1.0),
                                        errorPrecisionStartValue = 1.0,
                                        includeSourceEffect = TRUE,
                                        includeExposureEffect = TRUE,
                                        exposureEffectCount = 1,
                                        separateExposurePrior = FALSE,
                                        chainLength = 1100000,
                                        burnIn = 1e5,
                                        subSampleFrequency = 100) {
  settings <- list(
    primaryEffectPriorStd = primaryEffectPriorStd,
    secondaryEffectPriorStd = secondaryEffectPriorStd,
    globalExposureEffectPriorMean = globalExposureEffectPriorMean,
    globalExposureEffectPriorStd = globalExposureEffectPriorStd,
    primaryEffectPrecisionPrior = primaryEffectPrecisionPrior,
    secondaryEffectPrecisionPrior = secondaryEffectPrecisionPrior,
    errorPrecisionPrior = errorPrecisionPrior,
    errorPrecisionStartValue = errorPrecisionStartValue,
    includeSourceEffect = includeSourceEffect,
    includeExposureEffect = includeExposureEffect,
    exposureEffectCount = exposureEffectCount,
    separateExposurePrior = separateExposurePrior,
    chainLength = chainLength,
    burnIn = burnIn,
    subSampleFrequency = subSampleFrequency
  )
  class(settings) <- "BayesianHMASettings"
  return(settings)
}

# =============================================================================
# CohortIncidence Builder Functions
# =============================================================================

createIncidenceDesign <- function(cohortDefs = NULL,
                                  targetDefs = NULL,
                                  outcomeDefs = NULL,
                                  tars = NULL,
                                  analysisList = NULL,
                                  conceptSets = NULL,
                                  subgroups = NULL,
                                  strataSettings = NULL,
                                  studyWindow = NULL) {
  design <- list()
  if (!is.null(cohortDefs)) design$cohortDefs <- cohortDefs
  if (!is.null(targetDefs)) design$targetDefs <- targetDefs
  if (!is.null(outcomeDefs)) design$outcomeDefs <- outcomeDefs
  if (!is.null(tars)) design$timeAtRiskDefs <- tars
  if (!is.null(analysisList)) design$analysisList <- analysisList
  if (!is.null(conceptSets)) design$conceptSets <- conceptSets
  if (!is.null(subgroups)) design$subgroups <- subgroups
  if (!is.null(strataSettings)) design$strataSettings <- strataSettings
  if (!is.null(studyWindow)) design$studyWindow <- studyWindow
  class(design) <- "IncidenceDesign"
  return(design)
}

createIncidenceAnalysis <- function(targets, outcomes, tars) {
  analysis <- list(
    targets = targets,
    outcomes = outcomes,
    tars = tars
  )
  class(analysis) <- "IncidenceAnalysis"
  return(analysis)
}

createCohortRef <- function(id, name, description = NULL) {
  ref <- list(id = id, name = name)
  if (!is.null(description)) ref$description <- description
  class(ref) <- "CohortReference"
  return(ref)
}

createOutcomeDef <- function(id, name = NULL, cohortId = 0, cleanWindow = 0, excludeCohortId = NULL) {
  outcomeDef <- list(id = id, cohortId = cohortId, cleanWindow = cleanWindow)
  if (!is.null(name)) outcomeDef$name <- name
  if (!is.null(excludeCohortId)) outcomeDef$excludeCohortId <- excludeCohortId
  class(outcomeDef) <- "Outcome"
  return(outcomeDef)
}

createTimeAtRiskDef <- function(id, startWith = "start", startOffset = 0, endWith = "end", endOffset = 0) {
  tarDef <- list(
    id = id,
    startWith = startWith,
    startOffset = startOffset,
    endWith = endWith,
    endOffset = endOffset
  )
  class(tarDef) <- "TimeAtRisk"
  return(tarDef)
}

createCohortSubgroup <- function(id, name, description = NULL, cohortRef = NULL) {
  subgroup <- list(id = id, name = name)
  if (!is.null(description)) subgroup$description <- description
  if (!is.null(cohortRef)) subgroup$cohort <- cohortRef
  class(subgroup) <- "CohortSubgroup"
  return(subgroup)
}

createStrataSettings <- function(byAge = FALSE, byGender = FALSE, byYear = FALSE,
                                 ageBreaks = NULL, ageBreakList = NULL) {
  if (byAge && is.null(ageBreaks) && is.null(ageBreakList)) {
    stop("When byAge = TRUE, ageBreaks or ageBreakList must be provided.")
  }
  settings <- list(byAge = byAge, byGender = byGender, byYear = byYear)
  if (!is.null(ageBreaks)) settings$ageBreaks <- ageBreaks
  if (!is.null(ageBreakList)) settings$ageBreakList <- ageBreakList
  class(settings) <- "StrataSettings"
  return(settings)
}

createDateRange <- function(startDate = NULL, endDate = NULL) {
  dateRange <- list()
  if (!is.null(startDate)) dateRange$startDate <- startDate
  if (!is.null(endDate)) dateRange$endDate <- endDate
  class(dateRange) <- "DateRange"
  return(dateRange)
}

# =============================================================================
# Characterization Builder Functions
# =============================================================================

createCharacterizationSettings <- function(timeToEventSettings = NULL,
                                            dechallengeRechallengeSettings = NULL,
                                            aggregateCovariateSettings = NULL) {
  if (inherits(timeToEventSettings, "timeToEventSettings")) {
    timeToEventSettings <- list(timeToEventSettings)
  }
  if (inherits(dechallengeRechallengeSettings, "dechallengeRechallengeSettings")) {
    dechallengeRechallengeSettings <- list(dechallengeRechallengeSettings)
  }
  if (inherits(aggregateCovariateSettings, "aggregateCovariateSettings")) {
    aggregateCovariateSettings <- list(aggregateCovariateSettings)
  }
  settings <- list(
    timeToEventSettings = timeToEventSettings,
    dechallengeRechallengeSettings = dechallengeRechallengeSettings,
    aggregateCovariateSettings = aggregateCovariateSettings
  )
  class(settings) <- "characterizationSettings"
  return(settings)
}

createDechallengeRechallengeSettings <- function(targetIds,
                                                 outcomeIds,
                                                 dechallengeStopInterval = 30,
                                                 dechallengeEvaluationWindow = 30) {
  result <- list(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    dechallengeStopInterval = dechallengeStopInterval,
    dechallengeEvaluationWindow = dechallengeEvaluationWindow
  )
  class(result) <- "dechallengeRechallengeSettings"
  return(result)
}

createTimeToEventSettings <- function(targetIds, outcomeIds) {
  result <- list(
    targetIds = targetIds,
    outcomeIds = outcomeIds
  )
  class(result) <- "timeToEventSettings"
  return(result)
}

createAggregateCovariateSettings <- function(targetIds,
                                             outcomeIds,
                                             minPriorObservation = 0,
                                             outcomeWashoutDays = 0,
                                             riskWindowStart = 1,
                                             startAnchor = "cohort start",
                                             riskWindowEnd = 365,
                                             endAnchor = "cohort start",
                                             covariateSettings = .getDefaultCharacterizationCovariateSettings(),
                                             caseCovariateSettings = .getDefaultCaseCovariateSettings(),
                                             casePreTargetDuration = 365,
                                             casePostOutcomeDuration = 365,
                                             extractNonCaseCovariates = TRUE) {
  if (inherits(covariateSettings, "covariateSettings")) {
    covariateSettings <- list(covariateSettings)
  }
  result <- list(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    minPriorObservation = minPriorObservation,
    outcomeWashoutDays = outcomeWashoutDays,
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    covariateSettings = covariateSettings,
    caseCovariateSettings = caseCovariateSettings,
    casePreTargetDuration = casePreTargetDuration,
    casePostOutcomeDuration = casePostOutcomeDuration,
    extractNonCaseCovariates = extractNonCaseCovariates
  )
  class(result) <- "aggregateCovariateSettings"
  return(result)
}

createDuringCovariateSettings <- function(useConditionOccurrenceDuring = FALSE,
                                          useConditionOccurrencePrimaryInpatientDuring = FALSE,
                                          useConditionEraDuring = FALSE,
                                          useConditionGroupEraDuring = FALSE,
                                          useDrugExposureDuring = FALSE,
                                          useDrugEraDuring = FALSE,
                                          useDrugGroupEraDuring = FALSE,
                                          useProcedureOccurrenceDuring = FALSE,
                                          useDeviceExposureDuring = FALSE,
                                          useMeasurementDuring = FALSE,
                                          useObservationDuring = FALSE,
                                          useVisitCountDuring = FALSE,
                                          useVisitConceptCountDuring = FALSE,
                                          includedCovariateConceptIds = c(),
                                          addDescendantsToInclude = FALSE,
                                          excludedCovariateConceptIds = c(),
                                          addDescendantsToExclude = FALSE,
                                          includedCovariateIds = c()) {
  covariateSettings <- list(
    temporal = FALSE,
    temporalSequence = FALSE
  )
  formalNames <- names(formals(createDuringCovariateSettings))
  anyUseTrue <- FALSE
  for (name in formalNames) {
    value <- get(name)
    if (is.null(value)) value <- vector()
    if (grepl("^use", name)) {
      if (value) {
        covariateSettings[[sub("use", "", name)]] <- value
        anyUseTrue <- TRUE
      }
    } else {
      covariateSettings[[name]] <- value
    }
  }
  if (!anyUseTrue) {
    stop("No covariate analysis selected. Must select at least one")
  }
  attr(covariateSettings, "fun") <- "Characterization::getDbDuringCovariateData"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}

# =============================================================================
# Core Public Functions (User Story 1)
# =============================================================================

# T016: Create an empty analysis specifications object
#' @title Create Empty Analysis Specifications
#' @description Creates an empty analysis specifications object that can be
#' populated with shared resources and module specifications.
#' @return An object of type `AnalysisSpecifications`.
createEmptyAnalysisSpecifications <- function() {
  analysisSpecifications <- list(
    sharedResources = list(),
    moduleSpecifications = list()
  )
  class(analysisSpecifications) <- "AnalysisSpecifications"
  return(analysisSpecifications)
}

# T017: Add shared resources to analysis specifications
#' @title Add Shared Resources
#' @description Add shared resources (e.g., cohort definitions) to analysis specifications.
#' @param analysisSpecifications An object of type `AnalysisSpecifications`.
#' @param sharedResources An object of type `SharedResources`.
#' @return The `analysisSpecifications` object with the shared resources added.
addSharedResources <- function(analysisSpecifications, sharedResources) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(analysisSpecifications, "AnalysisSpecifications", add = errorMessages)
  checkmate::assertClass(sharedResources, "SharedResources", add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  analysisSpecifications$sharedResources[[length(analysisSpecifications$sharedResources) + 1]] <- sharedResources
  return(analysisSpecifications)
}

# T018: Add module specifications to analysis specifications
#' @title Add Module Specifications
#' @description Add module specifications to analysis specifications.
#' @param analysisSpecifications An object of type `AnalysisSpecifications`.
#' @param moduleSpecifications An object of type `ModuleSpecifications`.
#' @return The `analysisSpecifications` object with the module specifications added.
addModuleSpecifications <- function(analysisSpecifications, moduleSpecifications) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(analysisSpecifications, "AnalysisSpecifications", add = errorMessages)
  checkmate::assertClass(moduleSpecifications, "ModuleSpecifications", add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  analysisSpecifications$moduleSpecifications[[length(analysisSpecifications$moduleSpecifications) + 1]] <- moduleSpecifications
  return(analysisSpecifications)
}

# =============================================================================
# Shared Resource Functions (User Story 2)
# =============================================================================

# T023: Create cohort shared resource specifications
#' @title Create Cohort Shared Resource Specifications
#' @description Creates shared resource specifications for cohort definitions.
#' Replaces CohortGeneratorModule$createCohortSharedResourceSpecifications().
#' @param cohortDefinitionSet A data frame with columns cohortId, cohortName, sql, json.
#'   May optionally include subset columns (isSubset, subsetParent, subsetDefinitionId).
#' @return An object of class `CohortDefinitionSharedResources` and `SharedResources`.
createCohortSharedResourceSpecifications <- function(cohortDefinitionSet) {
  # Validate cohort definition set
  if (!.isCohortDefinitionSet(cohortDefinitionSet)) {
    stop("cohortDefinitionSet is not properly defined. Required columns: cohortId, cohortName, sql, json")
  }

  # Check for subset definitions
  hasSubsets <- "isSubset" %in% names(cohortDefinitionSet) &&
    any(cohortDefinitionSet$isSubset == TRUE, na.rm = TRUE)

  if (hasSubsets) {
    # Filter to parent cohorts only
    parentCohortDefinitionSet <- cohortDefinitionSet[!cohortDefinitionSet$isSubset, ]
  } else {
    parentCohortDefinitionSet <- cohortDefinitionSet
  }

  sharedResource <- list()

  # Convert parent cohorts to list format
  cohortDefinitionsList <- .listafy(parentCohortDefinitionSet)
  sharedResource[["cohortDefinitions"]] <- cohortDefinitionsList

  if (hasSubsets) {
    # Note: subsetDefs would normally contain JSON from CohortSubsetDefinition objects.
    # In WebR context, users must provide pre-serialized subset definitions if needed.
    # For now, we only handle the cohortSubsets mapping.

    # Filter to subsets
    subsetCohortDefinitionSet <- cohortDefinitionSet[cohortDefinitionSet$isSubset, ]

    # Create subset ID mapping
    subsetIdMapping <- list()
    for (i in seq_len(nrow(subsetCohortDefinitionSet))) {
      idMapping <- list(
        cohortId = subsetCohortDefinitionSet$cohortId[i],
        subsetId = subsetCohortDefinitionSet$subsetDefinitionId[i],
        targetCohortId = subsetCohortDefinitionSet$subsetParent[i]
      )
      subsetIdMapping[[i]] <- idMapping
    }
    sharedResource[["cohortSubsets"]] <- subsetIdMapping
  }

  sharedResource <- .createSharedResourcesSpecifications(
    className = "CohortDefinitionSharedResources",
    sharedResourcesSpecifications = sharedResource
  )
  return(sharedResource)
}

# T024: Create negative control outcome cohort shared resource specifications
#' @title Create Negative Control Outcome Cohort Shared Resource Specifications
#' @description Creates shared resource specifications for negative control outcome cohorts.
#' @param negativeControlOutcomeCohortSet A data frame with cohortId, cohortName, outcomeConceptId.
#' @param occurrenceType Either "first" or "all".
#' @param detectOnDescendants Logical. When TRUE, uses concept_ancestor table
#'   to detect descendant concepts when constructing the cohort.
#' @return An object of class `NegativeControlOutcomeSharedResources` and `SharedResources`.
createNegativeControlOutcomeCohortSharedResourceSpecifications <- function(negativeControlOutcomeCohortSet,
                                                                           occurrenceType,
                                                                           detectOnDescendants) {
  # Convert data frame rows to list of lists
  negativeControlOutcomeCohortSetList <- apply(negativeControlOutcomeCohortSet, 1, as.list)

  sharedResource <- list(
    negativeControlOutcomes = list(
      negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSetList,
      occurrenceType = occurrenceType,
      detectOnDescendants = detectOnDescendants
    )
  )

  sharedResource <- .createSharedResourcesSpecifications(
    className = "NegativeControlOutcomeSharedResources",
    sharedResourcesSpecifications = sharedResource
  )
  return(sharedResource)
}

# =============================================================================
# Module Specification Functions (User Story 3)
# =============================================================================

# T031: CohortGenerator Module Specifications
#' @title Create CohortGenerator Module Specifications
#' @description Creates module specifications for the CohortGenerator module.
#' @param generateStats When TRUE, inclusion rule statistics will be computed.
#' @return An object of class `ModuleSpecifications`.
createCohortGeneratorModuleSpecifications <- function(generateStats = TRUE) {
  moduleSettings <- list(
    generateStats = generateStats
  )
  return(.createModuleSpecifications("CohortGeneratorModule", moduleSettings))
}

# T032: CohortDiagnostics Module Specifications
#' @title Create CohortDiagnostics Module Specifications
#' @description Creates module specifications for the CohortDiagnostics module.
#' @param cohortIds Vector of cohort IDs to analyze. NULL means all cohorts.
#' @param runInclusionStatistics Run inclusion rule statistics.
#' @param runIncludedSourceConcepts Run included source concepts analysis.
#' @param runOrphanConcepts Run orphan concepts analysis.
#' @param runTimeSeries Run time series analysis.
#' @param runVisitContext Run visit context analysis.
#' @param runBreakdownIndexEvents Run breakdown index events analysis.
#' @param runIncidenceRate Run incidence rate analysis.
#' @param runCohortRelationship Run cohort relationship analysis.
#' @param runTemporalCohortCharacterization Run temporal cohort characterization.
#' @param temporalCovariateSettings Covariate settings for temporal analysis.
#'   Defaults to sensible temporal covariate settings.
#' @param minCharacterizationMean Minimum mean for characterization.
#' @param irWashoutPeriod Incidence rate washout period.
#' @return An object of class `ModuleSpecifications`.
createCohortDiagnosticsModuleSpecifications <- function(cohortIds = NULL,
                                                        runInclusionStatistics = TRUE,
                                                        runIncludedSourceConcepts = TRUE,
                                                        runOrphanConcepts = TRUE,
                                                        runTimeSeries = FALSE,
                                                        runVisitContext = TRUE,
                                                        runBreakdownIndexEvents = TRUE,
                                                        runIncidenceRate = TRUE,
                                                        runCohortRelationship = TRUE,
                                                        runTemporalCohortCharacterization = TRUE,
                                                        temporalCovariateSettings = .getDefaultTemporalCovariateSettings(),
                                                        minCharacterizationMean = 0.01,
                                                        irWashoutPeriod = 0) {
  moduleSettings <- list(
    cohortIds = cohortIds,
    runInclusionStatistics = runInclusionStatistics,
    runIncludedSourceConcepts = runIncludedSourceConcepts,
    runOrphanConcepts = runOrphanConcepts,
    runTimeSeries = runTimeSeries,
    runVisitContext = runVisitContext,
    runBreakdownIndexEvents = runBreakdownIndexEvents,
    runIncidenceRate = runIncidenceRate,
    runCohortRelationship = runCohortRelationship,
    runTemporalCohortCharacterization = runTemporalCohortCharacterization,
    temporalCovariateSettings = temporalCovariateSettings,
    minCharacterizationMean = minCharacterizationMean,
    irWashoutPeriod = irWashoutPeriod
  )
  return(.createModuleSpecifications("CohortDiagnosticsModule", moduleSettings))
}

# T033: CohortIncidence Module Specifications
#' @title Create CohortIncidence Module Specifications
#' @description Creates module specifications for the CohortIncidence module.
#' @param irDesign The incidence rate design from CohortIncidence package.
#' @return An object of class `ModuleSpecifications`.
createCohortIncidenceModuleSpecifications <- function(irDesign = NULL) {
  moduleSettings <- list(
    irDesign = irDesign
  )
  return(.createModuleSpecifications("CohortIncidenceModule", moduleSettings))
}

# T034: CohortMethod Module Specifications
#' @title Create CohortMethod Module Specifications
#' @description Creates module specifications for the CohortMethod module.
#' @param cmAnalysisList List of CohortMethod analysis settings.
#' @param targetComparatorOutcomesList List of target-comparator-outcomes.
#' @param analysesToExclude Analyses to exclude.
#' @param refitPsForEveryOutcome Refit propensity score for every outcome.
#' @param refitPsForEveryStudyPopulation Refit PS for every study population.
#' @param cmDiagnosticThresholds Diagnostic thresholds. Defaults to sensible values.
#' @return An object of class `ModuleSpecifications`.
createCohortMethodModuleSpecifications <- function(cmAnalysisList,
                                                   targetComparatorOutcomesList,
                                                   analysesToExclude = NULL,
                                                   refitPsForEveryOutcome = FALSE,
                                                   refitPsForEveryStudyPopulation = TRUE,
                                                   cmDiagnosticThresholds = .createDefaultCmDiagnosticThresholds()) {
  moduleSettings <- list(
    cmAnalysisList = cmAnalysisList,
    targetComparatorOutcomesList = targetComparatorOutcomesList,
    analysesToExclude = analysesToExclude,
    refitPsForEveryOutcome = refitPsForEveryOutcome,
    refitPsForEveryStudyPopulation = refitPsForEveryStudyPopulation,
    cmDiagnosticThresholds = cmDiagnosticThresholds
  )
  return(.createModuleSpecifications("CohortMethodModule", moduleSettings))
}

# T035: Characterization Module Specifications
#' @title Create Characterization Module Specifications
#' @description Creates module specifications for the Characterization module.
#' @param targetIds Vector of target cohort IDs.
#' @param outcomeIds Vector of outcome cohort IDs.
#' @param outcomeWashoutDays Days of washout for outcomes.
#' @param minPriorObservation Minimum prior observation days.
#' @param dechallengeStopInterval Dechallenge stop interval.
#' @param dechallengeEvaluationWindow Dechallenge evaluation window.
#' @param riskWindowStart Risk window start days.
#' @param startAnchor Risk window start anchor.
#' @param riskWindowEnd Risk window end days.
#' @param endAnchor Risk window end anchor.
#' @param minCharacterizationMean Minimum characterization mean.
#' @param covariateSettings Covariate settings. Defaults to characterization defaults.
#' @param caseCovariateSettings Case (during) covariate settings.
#' @return An object of class `ModuleSpecifications`.
createCharacterizationModuleSpecifications <- function(targetIds,
                                                       outcomeIds,
                                                       outcomeWashoutDays = c(365),
                                                       minPriorObservation = 365,
                                                       dechallengeStopInterval = 30,
                                                       dechallengeEvaluationWindow = 30,
                                                       riskWindowStart = c(1, 1),
                                                       startAnchor = c("cohort start", "cohort start"),
                                                       riskWindowEnd = c(0, 365),
                                                       endAnchor = c("cohort end", "cohort end"),
                                                       minCharacterizationMean = 0.01,
                                                       covariateSettings = .getDefaultCharacterizationCovariateSettings(),
                                                       caseCovariateSettings = .getDefaultCaseCovariateSettings()) {
  moduleSettings <- list(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    outcomeWashoutDays = outcomeWashoutDays,
    minPriorObservation = minPriorObservation,
    dechallengeStopInterval = dechallengeStopInterval,
    dechallengeEvaluationWindow = dechallengeEvaluationWindow,
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    minCharacterizationMean = minCharacterizationMean,
    covariateSettings = covariateSettings,
    caseCovariateSettings = caseCovariateSettings
  )
  return(.createModuleSpecifications("CharacterizationModule", moduleSettings))
}

# T036: PatientLevelPrediction Module Specifications
#' @title Create PatientLevelPrediction Module Specifications
#' @description Creates module specifications for the PatientLevelPrediction module.
#' @param modelDesignList List of model designs from PatientLevelPrediction.
#' @param skipDiagnostics Whether to skip diagnostics.
#' @return An object of class `ModuleSpecifications`.
createPatientLevelPredictionModuleSpecifications <- function(modelDesignList,
                                                             skipDiagnostics = FALSE) {
  moduleSettings <- list(
    modelDesignList = modelDesignList,
    skipDiagnostics = skipDiagnostics
  )
  return(.createModuleSpecifications("PatientLevelPredictionModule", moduleSettings))
}

# T037: PatientLevelPredictionValidation Module Specifications
#' @title Create PatientLevelPrediction Validation Module Specifications
#' @description Creates module specifications for the PLP Validation module.
#' @param validationList List of validation designs from PatientLevelPrediction.
#' @return An object of class `ModuleSpecifications`.
createPatientLevelPredictionValidationModuleSpecifications <- function(validationList) {
  moduleSettings <- list(
    validationList = validationList
  )
  return(.createModuleSpecifications("PatientLevelPredictionValidationModule", moduleSettings))
}

# T038: SelfControlledCaseSeries Module Specifications
#' @title Create SelfControlledCaseSeries Module Specifications
#' @description Creates module specifications for the SCCS module.
#' @param sccsAnalysesSpecifications SCCS analyses specifications from SCCS package.
#' @return An object of class `ModuleSpecifications`.
createSelfControlledCaseSeriesModuleSpecifications <- function(sccsAnalysesSpecifications) {
  moduleSettings <- list(
    sccsAnalysesSpecifications = sccsAnalysesSpecifications
  )
  return(.createModuleSpecifications("SelfControlledCaseSeriesModule", moduleSettings))
}

# T039: EvidenceSynthesis Module Specifications
#' @title Create EvidenceSynthesis Module Specifications
#' @description Creates module specifications for the EvidenceSynthesis module.
#' @param evidenceSynthesisAnalysisList List of evidence synthesis analyses.
#' @param esDiagnosticThresholds Diagnostic thresholds. Defaults to sensible values.
#' @return An object of class `ModuleSpecifications`.
createEvidenceSynthesisModuleSpecifications <- function(evidenceSynthesisAnalysisList,
                                                        esDiagnosticThresholds = .createDefaultEsDiagnosticThresholds()) {
  moduleSettings <- list(
    evidenceSynthesisAnalysisList = evidenceSynthesisAnalysisList,
    esDiagnosticThresholds = esDiagnosticThresholds
  )
  return(.createModuleSpecifications("EvidenceSynthesisModule", moduleSettings))
}

# T040: TreatmentPatterns Module Specifications
#' @title Create TreatmentPatterns Module Specifications
#' @description Creates module specifications for the TreatmentPatterns module.
#' @param cohorts Data frame with cohorts for treatment patterns analysis.
#' @param includeTreatments Treatment types to include.
#' @param indexDateOffset Offset from index date.
#' @param minEraDuration Minimum era duration.
#' @param splitEventCohorts Cohorts to split.
#' @param splitTime Time to split at.
#' @param eraCollapseSize Era collapse size.
#' @param combinationWindow Combination window.
#' @param minPostCombinationDuration Minimum post-combination duration.
#' @param filterTreatments Filter treatments ("First", "All", etc.).
#' @param maxPathLength Maximum path length.
#' @param ageWindow Age window for grouping.
#' @param minCellCount Minimum cell count.
#' @param censorType Censor type ("minCellCount", etc.).
#' @param overlapMethod Overlap method ("truncate", etc.).
#' @param concatTargets Concatenate targets.
#' @return An object of class `ModuleSpecifications`.
createTreatmentPatternsModuleSpecifications <- function(cohorts,
                                                        includeTreatments = NULL,
                                                        indexDateOffset = NULL,
                                                        minEraDuration = 0,
                                                        splitEventCohorts = NULL,
                                                        splitTime = NULL,
                                                        eraCollapseSize = 30,
                                                        combinationWindow = 30,
                                                        minPostCombinationDuration = 30,
                                                        filterTreatments = "First",
                                                        maxPathLength = 5,
                                                        ageWindow = 5,
                                                        minCellCount = 1,
                                                        censorType = "minCellCount",
                                                        overlapMethod = "truncate",
                                                        concatTargets = TRUE) {
  moduleSettings <- list(
    cohorts = cohorts,
    includeTreatments = includeTreatments,
    indexDateOffset = indexDateOffset,
    minEraDuration = minEraDuration,
    splitEventCohorts = splitEventCohorts,
    splitTime = splitTime,
    eraCollapseSize = eraCollapseSize,
    combinationWindow = combinationWindow,
    minPostCombinationDuration = minPostCombinationDuration,
    filterTreatments = filterTreatments,
    maxPathLength = maxPathLength,
    ageWindow = ageWindow,
    minCellCount = minCellCount,
    censorType = censorType,
    overlapMethod = overlapMethod,
    concatTargets = concatTargets
  )
  return(.createModuleSpecifications("TreatmentPatternsModule", moduleSettings))
}

# CohortSurvival Module Specifications
createCohortSurvivalModuleSpecifications <- function(targetCohortId,
                                                      outcomeCohortId,
                                                      strata = NULL,
                                                      analysisType = "single_event",
                                                      competingOutcomeCohortTable = NULL,
                                                      competingOutcomeCohortId = NULL,
                                                      outcomeDateVariable = "cohort_start_date",
                                                      outcomeWashout = Inf,
                                                      censorOnCohortExit = FALSE,
                                                      censorOnDate = NULL,
                                                      followUpDays = Inf,
                                                      eventGap = 30,
                                                      estimateGap = 1,
                                                      restrictedMeanFollowUp = NULL,
                                                      minimumSurvivalDays = 1) {
  moduleSettings <- list(
    targetCohortId = targetCohortId,
    outcomeCohortId = outcomeCohortId,
    strata = strata,
    analysisType = analysisType,
    competingOutcomeCohortTable = competingOutcomeCohortTable,
    competingOutcomeCohortId = competingOutcomeCohortId,
    outcomeDateVariable = outcomeDateVariable,
    outcomeWashout = outcomeWashout,
    censorOnCohortExit = censorOnCohortExit,
    censorOnDate = censorOnDate,
    followUpDays = followUpDays,
    eventGap = eventGap,
    estimateGap = estimateGap,
    restrictedMeanFollowUp = restrictedMeanFollowUp,
    minimumSurvivalDays = minimumSurvivalDays
  )
  return(.createModuleSpecifications("CohortSurvivalModule", moduleSettings))
}

# =============================================================================
# R5 Reference Class Wrappers (for notebook compatibility)
# =============================================================================
# Notebooks use CohortGeneratorModule$new(), TreatmentPatternsModule$new(),
# etc. These wrappers delegate to the standalone functions above.

CohortGeneratorModule <- setRefClass("CohortGeneratorModule",
  methods = list(
    createCohortSharedResourceSpecifications = function(cohortDefinitionSet) {
      createCohortSharedResourceSpecifications(cohortDefinitionSet)
    },
    createModuleSpecifications = function(generateStats = TRUE) {
      createCohortGeneratorModuleSpecifications(generateStats = generateStats)
    }
  )
)

CohortDiagnosticsModule <- setRefClass("CohortDiagnosticsModule",
  methods = list(
    createModuleSpecifications = function(...) {
      createCohortDiagnosticsModuleSpecifications(...)
    }
  )
)

CohortIncidenceModule <- setRefClass("CohortIncidenceModule",
  methods = list(
    createModuleSpecifications = function(irDesign = NULL) {
      createCohortIncidenceModuleSpecifications(irDesign = irDesign)
    }
  )
)

CohortMethodModule <- setRefClass("CohortMethodModule",
  methods = list(
    createModuleSpecifications = function(...) {
      createCohortMethodModuleSpecifications(...)
    }
  )
)

CharacterizationModule <- setRefClass("CharacterizationModule",
  methods = list(
    createModuleSpecifications = function(...) {
      createCharacterizationModuleSpecifications(...)
    }
  )
)

PatientLevelPredictionModule <- setRefClass("PatientLevelPredictionModule",
  methods = list(
    createModuleSpecifications = function(modelDesignList, ...) {
      createPatientLevelPredictionModuleSpecifications(modelDesignList = modelDesignList, ...)
    }
  )
)

SelfControlledCaseSeriesModule <- setRefClass("SelfControlledCaseSeriesModule",
  methods = list(
    createModuleSpecifications = function(sccsAnalysesSpecifications) {
      createSelfControlledCaseSeriesModuleSpecifications(sccsAnalysesSpecifications)
    }
  )
)

EvidenceSynthesisModule <- setRefClass("EvidenceSynthesisModule",
  methods = list(
    createModuleSpecifications = function(...) {
      createEvidenceSynthesisModuleSpecifications(...)
    }
  )
)

TreatmentPatternsModule <- setRefClass("TreatmentPatternsModule",
  methods = list(
    createModuleSpecifications = function(...) {
      createTreatmentPatternsModuleSpecifications(...)
    }
  )
)

CohortSurvivalModule <- setRefClass("CohortSurvivalModule",
  methods = list(
    createModuleSpecifications = function(...) {
      createCohortSurvivalModuleSpecifications(...)
    }
  )
)

# Typo-tolerant alias used in some notebooks
createEmptyAnalysisSpecificiations <- createEmptyAnalysisSpecifications

