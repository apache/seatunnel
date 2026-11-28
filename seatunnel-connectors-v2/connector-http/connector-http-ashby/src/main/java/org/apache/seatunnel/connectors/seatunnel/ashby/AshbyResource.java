/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.ashby;

import java.util.List;

/**
 * Read-only RPC methods and the continuation each one accepts.
 *
 * <p>Some single-response and cursor-only methods share a component response schema with
 * token-capable methods. Request capabilities decide how the source can continue.
 */
enum AshbyResource {
    APPLICATIONS("applications", "application.list", Mode.TOKEN, "candidatesRead"),
    APPLICATION_FEEDBACK(
            "application_feedback", "applicationFeedback.list", Mode.TOKEN, "candidatesRead"),
    ARCHIVE_REASONS(
            "archive_reasons",
            "archiveReason.list",
            Mode.SINGLE,
            "hiringProcessMetadataRead",
            "includeArchived"),
    CANDIDATE_TAGS(
            "candidate_tags",
            "candidateTag.list",
            Mode.TOKEN,
            "hiringProcessMetadataRead",
            "includeArchived"),
    CANDIDATES("candidates", "candidate.list", Mode.TOKEN, "candidatesRead"),
    CUSTOM_FIELDS(
            "custom_fields",
            "customField.list",
            Mode.TOKEN,
            "hiringProcessMetadataRead",
            "includeArchived"),
    DEPARTMENTS(
            "departments", "department.list", Mode.TOKEN, "organizationRead", "includeArchived"),
    FEEDBACK_FORM_DEFINITIONS(
            "feedback_form_definitions",
            "feedbackFormDefinition.list",
            Mode.TOKEN,
            "feedbackFormDefinitionsRead or hiringProcessMetadataRead",
            "includeArchived"),
    INTERVIEW_SCHEDULES(
            "interview_schedules", "interviewSchedule.list", Mode.TOKEN, "interviewsRead"),
    INTERVIEWS(
            "interviews",
            "interview.list",
            Mode.TOKEN,
            "interviewsRead",
            "includeArchived",
            "includeNonSharedInterviews"),
    INTERVIEW_STAGES("interview_stages", "interviewStage.list", Mode.SINGLE, "interviewsRead"),
    JOB_POSTINGS("job_postings", "jobPosting.list", Mode.SINGLE, "jobsRead"),
    JOBS("jobs", "job.list", Mode.TOKEN, "jobsRead"),
    LOCATIONS("locations", "location.list", Mode.TOKEN, "organizationRead", "includeArchived"),
    OFFERS("offers", "offer.list", Mode.TOKEN, "offersRead"),
    SOURCES("sources", "source.list", Mode.SINGLE, "hiringProcessMetadataRead", "includeArchived"),
    USERS("users", "user.list", Mode.TOKEN, "organizationRead", "includeDeactivated"),
    APPLICATION_CRITERIA_EVALUATIONS(
            "application_criteria_evaluations",
            "application.listCriteriaEvaluations",
            Mode.CURSOR,
            "candidatesRead"),
    APPLICATION_HISTORY(
            "application_history", "application.listHistory", Mode.CURSOR, "candidatesRead"),
    INTERVIEW_PLANS(
            "interview_plans",
            "interviewPlan.list",
            Mode.TOKEN,
            "interviewsRead",
            "includeArchived"),
    INTERVIEW_STAGE_GROUPS(
            "interview_stage_groups", "interviewStageGroup.list", Mode.SINGLE, "interviewsRead"),
    INTERVIEWER_POOLS(
            "interviewer_pools",
            "interviewerPool.list",
            Mode.TOKEN,
            "hiringProcessMetadataRead",
            "includeArchivedPools",
            "includeArchivedTrainingStages"),
    SURVEY_FORM_DEFINITIONS(
            "survey_form_definitions",
            "surveyFormDefinition.list",
            Mode.TOKEN,
            "hiringProcessMetadataRead"),
    SOURCE_TRACKING_LINKS(
            "source_tracking_links",
            "sourceTrackingLink.list",
            Mode.TOKEN,
            "hiringProcessMetadataRead",
            "includeDisabled"),
    OPENINGS("openings", "opening.list", Mode.TOKEN, "jobsRead"),
    JOB_TEMPLATES("job_templates", "jobTemplate.list", Mode.TOKEN, "jobsRead"),
    JOB_BOARDS("job_boards", "jobBoard.list", Mode.SINGLE, "jobsRead"),
    APPROVALS("approvals", "approval.list", Mode.TOKEN, "approvalsRead"),
    PROJECTS("projects", "project.list", Mode.TOKEN, "candidatesRead or projectsRead");

    /**
     * The accepted continuation parameters of a provider method.
     *
     * <p>Only TOKEN methods start another pass with their previous final sync token.
     */
    enum Mode {
        SINGLE,
        CURSOR,
        TOKEN
    }

    final String key;
    final String rpc;
    final Mode mode;
    final String permission;
    final List<String> flags;

    AshbyResource(String key, String rpc, Mode mode, String permission, String... flags) {
        this.key = key;
        this.rpc = rpc;
        this.mode = mode;
        this.permission = permission;
        this.flags = List.of(flags);
    }

    static AshbyResource select(String name) {
        for (AshbyResource resource : values()) {
            if (resource.key.equalsIgnoreCase(name)) {
                return resource;
            }
        }
        throw AshbySettings.invalid("resource", "expected a supported Ashby resource");
    }

    AshbyResource parent() {
        switch (this) {
            case APPLICATION_CRITERIA_EVALUATIONS:
            case APPLICATION_HISTORY:
                return APPLICATIONS;
            case INTERVIEW_STAGES:
                return INTERVIEW_PLANS;
            default:
                return null;
        }
    }

    boolean createdFilter() {
        return this == APPLICATIONS || this == APPLICATION_FEEDBACK || this == INTERVIEW_SCHEDULES;
    }

    boolean syncs() {
        return mode == Mode.TOKEN || this == APPLICATION_HISTORY;
    }
}
