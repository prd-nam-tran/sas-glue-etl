from itertools import chain

from pyspark.sql import functions as f, types

from olivia_glue.transform.constants import (
    OliviaTable,
    SASTable,
    WebWidgetType,
    ApplySource,
    ApplyType,
    WIDGET_SOURCES,
    ATSSystem,
    ExtraApplySource,
    LeadStatus,
    CommonConst,
)
from olivia_glue.transform.lead.base import BaseCandidateTransformJob

# In the default setup, there will be two workers of type G.1X with 4 cores and 16 GB of RAM.
# WORKERS = 7
# CORES = 16

# One worker will be kept for the driver, so the real number of executors here is the total of workers minus 1.
# n_partitions = int(CORES * (WORKERS - 1))

class CandidateTransformJob(BaseCandidateTransformJob):
    def _load_data_frames(self):
        if self.is_cdc_job:
            self._lead_df = self._fetch_data_by_cdc_ids(OliviaTable.AI_LEADS, "id", self._cdc_lead_ids)
            self._lead_df.cache()
        else:
            self._lead_df = self._fetch_data(OliviaTable.AI_LEADS)

        if not self._lead_df.isEmpty():
            self._lead_extra_data_df = self._fetch_data(OliviaTable.AI_LEAD_EXTRA_DATA)
            self._candidate_profile_df = self._fetch_data(OliviaTable.AI_CANDIDATE_PROFILES)
            self._event_itv_lead_df = self._fetch_data(OliviaTable.AI_EVENT_ITV_LEADS)
            self._journey_stage_df = self._fetch_data(OliviaTable.AI_JOURNEY_STAGES)
            self._stage_status_df = self._fetch_data(OliviaTable.AI_STAGE_STATUSES)
            self._company_area_df = self._fetch_data(OliviaTable.AI_COMPANY_AREAS)
            self._job_loc_df = self._fetch_data(OliviaTable.AI_JOB_LOCS)
            self._previous_application_df = self._fetch_data(OliviaTable.AI_PREVIOUS_APPLICATIONS)
            self._job_df = self._fetch_data(OliviaTable.AI_JOBS)
            self._job_location_df = self._fetch_data(OliviaTable.AI_JOB_LOCATIONS)
            self._company_brand_df = self._fetch_data(OliviaTable.AI_COMPANY_BRANDS)
            self._live_job_df = self._fetch_data(OliviaTable.AI_LIVE_JOBS)
            self._logging_df = self._fetch_data(OliviaTable.AI_LOGGING)
            self._interview_df = self._fetch_data(OliviaTable.AI_INTERVIEWS)
            self._applied_job_df = self._fetch_data(OliviaTable.AI_APPLIED_JOBS)
            self._conversation_action_df = self._fetch_data(OliviaTable.AI_CONVERSATION_ACTIONS)
            self._recorded_interview_candidate_df = self._fetch_data(OliviaTable.AI_RECORDED_INTERVIEW_CANDIDATE)
            self._recorded_interview_msg_df = self._fetch_data(OliviaTable.AI_RECORDED_INTERVIEW_MESSAGES)
            self._meeting_df = self._fetch_data(OliviaTable.AI_MEETINGS)
            self._contact_df = self._fetch_data(OliviaTable.AI_CONTACTS)
            self._company_user_df = self._fetch_data(OliviaTable.AI_COMPANY_USERS)
            self._event_candidate_df = self._fetch_data(OliviaTable.AI_EVENT_CANDIDATES)
            self._event_df = self._fetch_data(OliviaTable.AI_EVENTS)
            self._widget_df = self._fetch_data(OliviaTable.AI_WIDGETS)
            self._campaign_contact_df = self._fetch_data(OliviaTable.AI_CAMPAIGN_CONTACTS)
            self._outbound_campaign_df = self._fetch_data(OliviaTable.AI_OUTBOUND_CAMPAIGNS)
            self._authtool_user_df = self._fetch_data(OliviaTable.AUTHTOOLS_USER)
            self._employee_df = self._fetch_data(OliviaTable.AI_EMPLOYEES)
            self._external_map_id_df = self._fetch_data(OliviaTable.AI_EXTERNAL_MAP_IDS)
            self._job_family_df = self._fetch_data(OliviaTable.AI_JOB_FAMILIES)
            self._itv_pre_attendee_df = self._fetch_data(OliviaTable.AI_INTERVIEW_PRE_ATTENDEES)
            self._acl_role_df = self._fetch_data(OliviaTable.AI_ACL_ROLES)
            self._language_df = self._fetch_data(OliviaTable.AI_LANGUAGES)
            self._applied_job_extra_data = self._fetch_data(OliviaTable.AI_APPLIED_JOB_EXTRA_DATA)
            self._applied_job_extension = self._fetch_data(OliviaTable.AI_APPLIED_JOB_EXTENSION)

    def _do_transform(self):
        # Transform candidate default data
        self._transform_candidate_general_data()

        # Transform candidate extra data
        self._transform_latest_apply_job()
        self._transform_leads_specific_recorded_video()
        self._transform_lead_logs_applied_job()
        self._transform_logs_journey_completed_at()
        self._transform_last_interviews()
        self._transform_lead_meeting_counter()
        self._transform_leads_multi_apply()
        self._transform_lead_itv_pre_attendee()
        self._transform_leads_sources()
        self._transform_candidate_language()
        self._transform_applied_job_extra_data()
        

        # print(f"[INFO] Re-partitioning with {n_partitions} parts ...")
        # self._filter_candidate_df = self._filter_candidate_df.repartition(n_partitions)
        # self._data_candidate_df = self._data_candidate_df.repartition(n_partitions)

        self._write_to_s3_sink(self._filter_candidate_df, SASTable.SAS_FILTER_CANDIDATE)
        self._write_to_s3_sink(self._data_candidate_df, SASTable.SAS_DATA_CANDIDATE)

    def _transform_candidate_general_data(self):
        results_df = (
            self._lead_df.join(
                self._lead_extra_data_df,
                self._lead_df.id == self._lead_extra_data_df.lead_id,
                "left",
            )
            .join(
                self._candidate_profile_df,
                self._lead_df.candidate_profile_id == self._candidate_profile_df.id,
                "left",
            )
            .join(
                self._event_itv_lead_df,
                self._lead_df.id == self._event_itv_lead_df.lead_id,
                "left",
            )
            .join(
                self._journey_stage_df,
                [
                    self._lead_extra_data_df.journey_stage_id == self._journey_stage_df.id,
                    self._lead_df.company_id == self._journey_stage_df.company_id,
                ],
                "left",
            )
            .join(
                self._stage_status_df,
                [
                    self._journey_stage_df.id == self._stage_status_df.stage_id,
                    self._lead_extra_data_df.journey_status_id == self._stage_status_df.id,
                ],
                "left",
            )
            .join(
                self._company_area_df,
                [
                    self._lead_df.job_loc_id == self._company_area_df.job_loc_id,
                    self._lead_df.company_id == self._company_area_df.company_id,
                ],
                "left",
            )
            .join(
                self._job_loc_df,
                [
                    self._lead_df.job_loc_id == self._job_loc_df.id,
                    self._job_loc_df.is_deleted == 0,
                ],
                "left",
            )
            .join(
                self._previous_application_df,
                self._lead_df.id == self._previous_application_df.application_id,
                "left",
            )
            .join(
                self._job_df,
                [
                    self._lead_extra_data_df.job_req_id == self._job_df.job_req_id,
                    self._job_df.base_id > 0,
                ],
                "left",
            )
            .join(
                self._job_location_df,
                [
                    self._job_df.id == self._job_location_df.job_id,
                    self._company_area_df.id == self._job_location_df.company_area_id,
                ],
                "left",
            )
            .join(
                self._company_brand_df,
                [
                    self._job_df.company_brand_id == self._company_brand_df.id,
                    self._company_brand_df.status == 1,
                ],
                "left",
            )
            .join(
                self._live_job_df,
                [
                    self._job_df.id == self._live_job_df.job_id,
                    self._job_df.job_req_id == self._live_job_df.job_req_id,
                    self._job_loc_df.id == self._live_job_df.job_loc_id,
                ],
                "left",
            )
        )
        # results_df.cache()

        lead_name_expr = f.lower(f.coalesce(self._lead_df.name, f.lit("")))
        self._filter_candidate_df = results_df \
            .withColumn("is_valid_name_and_status",
                        (self._lead_df.status != LeadStatus.REMOVED)
                        & (lead_name_expr != "") & (~lead_name_expr.ilike("%test%")) & (~lead_name_expr.ilike("%unknown%"))) \
            .select([
                self._lead_df.id.alias("lead_id"),
                self._lead_df.company_id.alias("company_id"),
                self._lead_df.long_id.alias("long_id"),
                self._lead_df.conversation_id.alias("conversation_id"),
                self._lead_df.company_group_id.alias("company_group_id"),
                self._lead_df.candidate_profile_id.alias("candidate_profile_id"),
                self._lead_df.apply_type.alias("lead_apply_type"),
                self._lead_df.created_at.alias("lead_created_at"),
                self._lead_df.updated_at.alias("lead_updated_at"),
                self._lead_df.interviewer_ids.alias("lead_interviewer_ids"),
                self._lead_df.is_valid.alias("lead_is_valid"),
                self._lead_df.external_source_id.alias("lead_external_source_id"),
                self._lead_df.referred_by.alias("lead_referred_by"),
                self._lead_df.added_by.alias("lead_added_by"),
                self._lead_df.name.alias("lead_name"),
                self._lead_df.phone_number.alias("lead_phone_number"),
                self._lead_df.email.alias("lead_email"),
                self._lead_df.status.alias("lead_status"),
                self._lead_df.ip_addr.alias("ip_addr"),
                self._lead_df.start_kw.alias("lead_start_kw"),
                self._lead_df.source.alias("lead_source"),
                self._lead_df.tz_str.alias("lead_tz_str"),
                self._lead_extra_data_df.id.alias("lead_extra_id"),
                self._job_loc_df.location_id.alias("location_id"),
                self._company_area_df.id.alias("area_id"),
                self._job_df.id.alias("job_id"),
                self._job_df.name.alias("job_name"),
                self._job_loc_df.id.alias("job_loc_id"),
                self._company_brand_df.id.alias("job_brand_id"),
                self._live_job_df.id.alias("live_job_id"),
                self._event_itv_lead_df.id.alias("event_itv_leads_id"),
                self._journey_stage_df.id.alias("journey_stage_id"),
                self._stage_status_df.id.alias("stage_status_id"),
                self._lead_extra_data_df.application_id.alias("extra_application_id"),
                self._lead_extra_data_df.job_req_id.alias("extra_job_req_id"),
                self._lead_extra_data_df.job_title.alias("extra_job_title"),
                self._lead_extra_data_df.journey_status_id.alias("extra_journey_status_id"),
                self._candidate_profile_df.public_id.alias("profile_public_id"),
                self._candidate_profile_df.phone_number.alias("profile_phone_number"),
                self._candidate_profile_df.email.alias("profile_email"),
                "is_valid_name_and_status"
            ])

        self._data_candidate_df = results_df.select(
            [
                self._lead_df.id.alias("lead_id"),
                self._lead_df.company_id.alias("company_id"),
                self._lead_df.web_id.alias("widget_id"),
                self._lead_df.completed.alias("lead_completed"),
                self._lead_df.detect_language_code.alias("lead_detect_language_code"),
                self._lead_df.language_code.alias("lead_language_code"),
                self._lead_df.itv_requested_at.alias("lead_itv_requested_at"),
                self._lead_df.responsed.alias("lead_responsed"),
                self._lead_df.recent_ai_phone_number.alias("lead_recent_ai_phone_number"),
                self._lead_df.past_interviewer_ids.alias("lead_past_interviewer_ids"),
                self._lead_df.opt_in_status.alias("lead_opt_in_status"),
                self._lead_df.outbound_accepted.alias("lead_outbound_accepted"),
                self._lead_df.unsubscribed.alias("lead_unsubscribed"),
                self._lead_df.last_contacted_at_timestamp.alias("lead_last_contacted_at_timestamp"),
                self._lead_df.primary_contact_method.alias("lead_primary_contact_method"),
                self._lead_df.referrer.alias("lead_referrer"),
                self._previous_application_df.id.alias("previous_applications_id"),
                self._job_location_df.id.alias("job_location_id"),
                self._event_itv_lead_df.id.alias("event_itv_id"),
                self._lead_extra_data_df.external_info.alias("extra_external_info"),
                self._lead_extra_data_df.consent_to_marketing.alias("extra_consent_to_marketing"),
                self._lead_extra_data_df.apply_url.alias("extra_apply_url"),
                self._lead_extra_data_df.journey_candidate_status.alias("extra_candidate_journey_status"),
                self._lead_extra_data_df.itv_scheduling_method.alias("extra_itv_scheduling_method").cast(types.IntegerType()),
                self._previous_application_df.reapply_type.alias("previous_application_reapply_type"),
                self._event_itv_lead_df.checked_in.alias("event_itv_lead_checked_in"),
                self._event_itv_lead_df.status.alias("event_itv_lead_status"),
                self._event_itv_lead_df.selected_time_at.alias("event_itv_lead_selected_time_at"),
                self._event_itv_lead_df.requested_at.alias("event_itv_lead_requested_at"),
            ]
        )

    def _transform_latest_apply_job(self):
        """
        Refer SQL
        olivia_analytics/Apps/transform/sql/lead/latest_apply_job.sql
        """

        max_aaj_df = (
            self._applied_job_df.withColumn(
                "max_valid_created_at",
                f.when(f.col("is_valid") == 1, f.col("created_at")).otherwise(None).cast("timestamp"),
            )
            .withColumn("max_created_at", f.col("created_at").cast("timestamp"))
            .groupBy("lead_id")
            .agg(
                f.max("max_valid_created_at").alias("max_valid_created_at"),
                f.max("max_created_at").alias("max_created_at"),
            )
            .orderBy("lead_id")
            .select(
                [
                    f.col("lead_id").alias("candidate_id"),
                    "max_valid_created_at",
                    "max_created_at",
                ]
            )
        )

        conditions = [
            self._applied_job_df.lead_id == max_aaj_df.candidate_id,
            f.coalesce(max_aaj_df.max_valid_created_at, max_aaj_df.max_created_at) == self._applied_job_df.created_at,
            f.when(
                max_aaj_df.max_valid_created_at.isNotNull(),
                self._applied_job_df.is_valid == 1,
            ).otherwise(True),
        ]

        latest_apply_job_df = self._applied_job_df.join(max_aaj_df, conditions, "inner").select(
            [
                self._applied_job_df.id.alias("latest_apply_job_id"),
                self._applied_job_df.lead_id.alias("lead_id"),
            ]
        )

        self._filter_candidate_df = self._filter_candidate_df.join(latest_apply_job_df, ["lead_id"], "left")

    def _transform_leads_specific_recorded_video(self):
        """
        Refer SQL
        olivia_analytics/Apps/transform/sql/lead/lead_specific_recorded_video.sql
        """

        recorded_video_df = (
            self._recorded_interview_candidate_df.join(
                self._recorded_interview_msg_df,
                self._recorded_interview_candidate_df.id == self._recorded_interview_msg_df.recorded_itv_candidate_id,
                "inner",
            )
            .filter(self._recorded_interview_candidate_df.status == 2)
            .groupBy(self._recorded_interview_candidate_df.lead_id)
            .agg(
                f.count(f.when(self._recorded_interview_msg_df.message_type == 2, 1).otherwise(None)).alias("recorded_question_total"),
                f.count(f.when(self._recorded_interview_msg_df.message_type != 2, 1).otherwise(None)).alias("recorded_response_count"),
                f.min(
                    f.when(
                        self._recorded_interview_msg_df.message_type != 2,
                        self._recorded_interview_msg_df.created_at,
                    ).otherwise(None)
                ).alias("min_created_at"),
                f.max(
                    f.when(
                        self._recorded_interview_msg_df.message_type != 2,
                        self._recorded_interview_msg_df.created_at,
                    ).otherwise(None)
                ).alias("max_created_at"),
            )
            .withColumn(
                "recorded_within_24h",
                f.col("max_created_at").cast("long") - f.col("min_created_at").cast("long"),
            )
            .select(
                [
                    self._recorded_interview_candidate_df.lead_id,
                    "recorded_question_total",
                    "recorded_response_count",
                    "recorded_within_24h",
                ]
            )
        )

        self._filter_candidate_df = self._filter_candidate_df.join(recorded_video_df, ["lead_id"], "left")

    def _transform_logs_journey_completed_at(self):
        """
        Refer SQL
        olivia_analytics/Apps/transform/sql/lead/logs_journey_completed_at.sql
        """

        leads_journey_completed_df = self._logging_df \
            .filter((self._logging_df.module == "JOURNEY") & (self._logging_df.action == "Capture: Capture Complete")) \
            .groupBy("candidate_id") \
            .agg(f.min("id").alias("min_id"))

        results_df = (
            self._logging_df.alias("logging")
            .join(
                leads_journey_completed_df,
                leads_journey_completed_df.min_id == self._logging_df.id,
                "inner",
            )
            .select(
                [
                    f.col("logging.candidate_id").alias("lead_id"),
                    f.col("logging.created_at").alias("journey_completed_at"),
                ]
            )
        )

        self._data_candidate_df = self._data_candidate_df.join(results_df, ["lead_id"], "left")

    def _transform_last_interviews(self):
        """
        Refer SQL
        olivia_analytics/Apps/transform/sql/lead/last_itv.sql
        """
        last_interviews_df = (
            self._interview_df.groupBy("candidate_id")
            .agg(f.max("id").alias("last_itv_id"))
            .select(
                [
                    self._interview_df.candidate_id.alias("lead_id"),
                    "last_itv_id",
                ]
            )
        )

        self._data_candidate_df = self._data_candidate_df.join(last_interviews_df, ["lead_id"], "left")

    def _transform_lead_meeting_counter(self):
        """
        Refer SQL
        olivia_analytics/Apps/transform/sql/lead/meeting.sql
        """
        meetings_df = self._meeting_df.filter((f.col("external_id") != "") & (f.col("provider") > 0))

        meeting_counter_df = (
            meetings_df.groupBy("lead_id")
            .agg(
                f.sum(f.when(f.col("duration").isNull(), 0).otherwise(f.col("duration"))).alias("total_video_duration"),
                f.count(f.col("id")).alias("total_video_meetings"),
            )
            .select(
                [
                    "lead_id",
                    "total_video_duration",
                    "total_video_meetings",
                ]
            )
        )

        self._data_candidate_df = self._data_candidate_df.join(meeting_counter_df, ["lead_id"], "left")

    def _transform_leads_sources(self):
        """
        Refer SQL
        olivia_analytics/Apps/transform/sql/lead/leads_sources.sql
        """

        ats_system_json = ATSSystem.get_ats_systems_json()
        mapping_ats_system = f.create_map([f.lit(x) for x in chain(*ats_system_json.items())])

        # Collect leads sources
        leads_sources_df = self.__collect_leads_sources()

        data_lead_sources_df = leads_sources_df.select(
            [
                f.col("lead.id").alias("lead_id"),
                f.col("source_referred_by.name").alias("referred_by_name"),
                f.col("source_referred_by.email").alias("referred_by_email"),
                f.col("source_referred_by.phone_number").alias("referred_by_phone"),
                f.col("contact_source_add_by.name").alias("add_by_name"),
                f.col("contact_source_add_by.phone_number").alias("add_by_phone"),
                f.col("user_source_add_by.job_email").alias("add_by_email"),
                f.col("source_external.ex_id_1").alias("external_id_1"),
                f.col("source_external.ex_id_2").alias("external_id_2"),
                f.col("source_external.ex_id_3").alias("external_id_3"),
                f.col("source_external.ex_source").alias("ex_source"),
                f.col("web_widget.compsite_slug").alias("compsite_slug"),
                f.col("web_widget.widget_type").alias("widget_type"),
                f.col("live_job.external_job_slug").alias("external_job_slug"),
                f.when(
                    f.when(
                        (f.col("lead.referred_by") > 0) & f.col("source_referred_by.name").isNotNull(),
                        ExtraApplySource.REFERRED_BY,
                    ).otherwise(
                        f.when(
                            f.col("lead.apply_type") == ApplyType.EVENT,
                            ExtraApplySource.EVENT,
                        ).otherwise(
                            f.when(
                                f.col("web_widget.widget_type").isNotNull() & (f.col("lead.web_id") > 0) & (f.col("lead.source") == ApplySource.COMPANY_PAGE),
                                f.when(
                                    f.col("web_widget.widget_type") == WebWidgetType.COMPANY_ADDITIONAL,
                                    ExtraApplySource.LANDING_SITE,
                                ).otherwise(ApplySource.COMPANY_PAGE),
                            ).otherwise(0)
                        )
                    )
                    == 0,
                    f.when(f.col("lead.source") > 0, f.col("lead.source")).otherwise(0),
                )
                .otherwise(
                    f.when(
                        (f.col("lead.referred_by") > 0) & f.col("source_referred_by.name").isNotNull(),
                        ExtraApplySource.REFERRED_BY,
                    ).otherwise(
                        f.when(
                            (f.col("lead.apply_type") == ApplyType.EVENT) & (f.col("lead.source") == ApplySource.SMS),
                            ApplySource.SMS,
                        ).otherwise(
                            f.when(
                                (f.col("lead.apply_type") == ApplyType.EVENT) & (f.col("lead.source") == ApplySource.WIDGET),
                                ApplySource.WIDGET,
                            ).otherwise(
                                f.when(
                                    f.col("lead.apply_type") == ApplyType.EVENT,
                                    ExtraApplySource.EVENT,
                                ).otherwise(
                                    f.when(
                                        f.col("web_widget.widget_type").isNotNull() & (f.col("lead.web_id") > 0) & (f.col("lead.source") == ApplySource.COMPANY_PAGE),
                                        f.when(
                                            f.col("web_widget.widget_type") == WebWidgetType.COMPANY_ADDITIONAL,
                                            ExtraApplySource.LANDING_SITE,
                                        ).otherwise(ApplySource.COMPANY_PAGE),
                                    ).otherwise(0)
                                )
                            )
                        )
                    )
                )
                .alias("event_apply_source"),
                f.when(
                    (f.col("lead.referred_by") > 0) & f.col("source_referred_by.name").isNotNull(),
                    f.concat(f.lit("Referred by "), f.col("source_referred_by.name")),
                )
                .otherwise(
                    f.when(
                        (f.col("lead.apply_type") == ApplyType.EVENT) & (f.col("lead.source") == ApplySource.SMS),
                        f.coalesce(f.col("lead.location_number"), f.col("lead.start_kw")),
                    ).otherwise(
                        f.when(
                            (f.col("lead.apply_type") == ApplyType.EVENT) & (f.col("lead.source") == ApplySource.WIDGET),
                            "Event Landing Page",
                        ).otherwise(
                            f.when(
                                f.col("lead.apply_type") == ApplyType.EVENT,
                                f.col("source_event.name"),
                            ).otherwise(
                                f.when(
                                    f.col("lead.source").isin(WIDGET_SOURCES),
                                    f.when(
                                        f.col("web_widget.name").isNotNull(),
                                        f.col("web_widget.name"),
                                    ).otherwise(
                                        f.when(
                                            f.col("source_widget_type.name").isNotNull(),
                                            f.col("source_widget_type.name"),
                                        ).otherwise("")
                                    ),
                                ).otherwise(
                                    f.when(
                                        f.col("lead.source").isin(ApplySource.SMS, ApplySource.WHATSAPP) & (f.col("lead.location_number") != ""),
                                        f.col("lead.location_number"),
                                    ).otherwise(
                                        f.when(
                                            f.col("lead.source").isin(
                                                ApplySource.ADD_MANUALLY,
                                                ApplySource.OUTBOUND,
                                            )
                                            & (f.col("contact_source_add_by.name").isNotNull() | f.col("auth_user_add_by.name").isNotNull())
                                            & f.col("source_external.ex_source").isNull(),
                                            f.concat(
                                                f.lit("Added by "),
                                                f.coalesce(
                                                    f.col("contact_source_add_by.name"),
                                                    f.col("auth_user_add_by.name"),
                                                ),
                                            ),
                                        ).otherwise(
                                            f.when(
                                                f.col("lead.source").isin(
                                                    ApplySource.PUBLIC_API,
                                                    ApplySource.OUTBOUND,
                                                )
                                                & f.col("source_external.ex_source").isNotNull(),
                                                mapping_ats_system[f.col("source_external.ex_source")],
                                            ).otherwise(
                                                f.when(
                                                    (f.col("lead.source") == ApplySource.CAMPAIGN) & f.col("source_campaign.name").isNotNull(),
                                                    f.col("source_campaign.name"),
                                                ).otherwise("")
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                .alias("event_source_name"),
            ]
        )

        filter_lead_sources_df = leads_sources_df.select(
            [
                f.col("lead.id").alias("lead_id"),
                f.when(
                    (f.col("lead.referred_by") > 0) & f.col("source_referred_by.name").isNotNull(),
                    f.concat(f.lit("Referred by "), f.col("source_referred_by.name")),
                )
                .otherwise(
                    f.when(
                        f.col("lead.apply_type") == ApplyType.EVENT,
                        f.col("source_event.name"),
                    ).otherwise(
                        f.when(
                            f.col("lead.source").isin(WIDGET_SOURCES),
                            f.when(
                                f.col("web_widget.name").isNotNull(),
                                f.col("web_widget.name"),
                            ).otherwise(
                                f.when(
                                    f.col("source_widget_type.name").isNotNull(),
                                    f.col("source_widget_type.name"),
                                ).otherwise("")
                            ),
                        ).otherwise(
                            f.when(
                                f.col("lead.source").isin(ApplySource.SMS, ApplySource.WHATSAPP) & (f.col("lead.recent_ai_phone_number") != ""),
                                f.col("lead.recent_ai_phone_number"),
                            ).otherwise(
                                f.when(
                                    f.col("lead.source").isin(ApplySource.ADD_MANUALLY, ApplySource.OUTBOUND, ApplySource.BROWSER_EXTENSION)
                                    & (f.col("contact_source_add_by.name").isNotNull() | f.col("auth_user_add_by.name").isNotNull()),
                                    f.concat(
                                        f.lit("Added by "),
                                        f.coalesce(
                                            f.col("contact_source_add_by.name"),
                                            f.col("auth_user_add_by.name"),
                                        ),
                                    ),
                                ).otherwise(
                                    f.when(
                                        f.col("lead.source").isin(ApplySource.PUBLIC_API, ApplySource.OUTBOUND)
                                        & (f.col("source_external.ex_source").isNotNull()),
                                        mapping_ats_system[f.col("source_external.ex_source")],
                                    ).otherwise(
                                        f.when(
                                            (f.col("lead.source") == ApplySource.CAMPAIGN) & f.col(
                                                "source_campaign.name").isNotNull(),
                                            f.col("source_campaign.name"),
                                        ).otherwise("")
                                    )
                                )
                            )
                        )
                    )
                )
                .alias("source_name"),
                f.when(
                    (f.col("web_widget.widget_type") == WebWidgetType.JOB_POSTING) & (f.col("lead.source") == ApplySource.WIDGET),
                    ExtraApplySource.PARADOX_JOB_POSTING_SITE,
                )
                .otherwise(
                    f.when(
                        f.when(
                            f.col("lead.referred_by") > 0,
                            ExtraApplySource.REFERRED_BY,
                        ).otherwise(
                            f.when(
                                f.col("lead.apply_type") == ApplyType.EVENT,
                                ExtraApplySource.EVENT,
                            ).otherwise(
                                f.when(
                                    f.col("web_widget.widget_type").isNotNull() & (f.col("lead.web_id") > 0) & (f.col("lead.source") == ApplySource.COMPANY_PAGE),
                                    f.when(
                                        f.col("web_widget.widget_type") == WebWidgetType.COMPANY_ADDITIONAL,
                                        ApplySource.LANDING_SITE,
                                    ).otherwise(ApplySource.COMPANY_PAGE),
                                ).otherwise(0)
                            )
                        )
                        == 0,
                        f.when(f.col("lead.source") > 0, f.col("lead.source")).otherwise(0),
                    ).otherwise(
                        f.when(
                            f.col("lead.referred_by") > 0,
                            ExtraApplySource.REFERRED_BY,
                        ).otherwise(
                            f.when(
                                f.col("lead.apply_type") == ApplyType.EVENT,
                                ExtraApplySource.EVENT,
                            ).otherwise(
                                f.when(
                                    f.col("web_widget.widget_type").isNotNull() & (f.col("lead.web_id") > 0) & (f.col("lead.source") == ApplySource.COMPANY_PAGE),
                                    f.when(
                                        f.col("web_widget.widget_type") == WebWidgetType.COMPANY_ADDITIONAL,
                                        ApplySource.LANDING_SITE,
                                    ).otherwise(ApplySource.COMPANY_PAGE),
                                ).otherwise(0)
                            )
                        )
                    )
                )
                .alias("apply_source"),
            ]
        )

        self._filter_candidate_df = self._filter_candidate_df.join(filter_lead_sources_df, ["lead_id"], "left")
        self._data_candidate_df = self._data_candidate_df.join(data_lead_sources_df, ["lead_id"], "left")

    def __collect_source_event(self):
        return (
            self._event_candidate_df.alias("event_cand")
            .join(
                self._event_df.alias("evt"),
                f.col("evt.id") == f.col("event_cand.event_id"),
                "inner",
            )
            .groupBy(f.col("evt.company_id"), f.col("event_cand.lead_id"))
            .agg(f.max(f.col("evt.name")).alias("name"))
            .select(
                [
                    "name",
                    f.col("event_cand.lead_id").alias("lead_id"),
                    f.col("evt.company_id").alias("company_id"),
                ]
            )
        )

    def __collect_max_widget(self):
        return (
            self._widget_df.filter(
                self._widget_df.widget_type.isin(
                    WebWidgetType.COMPANY_SITE,
                    WebWidgetType.ERP_SITE,
                    WebWidgetType.SCHEDULE,
                )
            )
            .groupBy(
                self._widget_df.company_id,
                self._widget_df.widget_type,
            )
            .agg(f.max("id").alias("max_id"))
            .select(
                [
                    "company_id",
                    "widget_type",
                    "max_id",
                ]
            )
        )

    def __collect_source_widget_type(self):
        # Collect max widget
        max_widget_df = self.__collect_max_widget()

        return (
            self._widget_df.alias("widget")
            .join(
                max_widget_df.alias("max_widget"),
                [
                    f.col("max_widget.max_id") == f.col("widget.id"),
                    f.col("max_widget.company_id") == f.col("widget.company_id"),
                    f.col("max_widget.widget_type").isin(
                        WebWidgetType.COMPANY_SITE,
                        WebWidgetType.ERP_SITE,
                        WebWidgetType.SCHEDULE,
                    ),
                ],
                "inner",
            )
            .select(
                [
                    f.col("widget.name").alias("name"),
                    f.col("widget.widget_type").alias("widget_type"),
                    f.col("widget.company_id").alias("company_id"),
                ]
            )
        )

    def __collect_source_campaign(self):
        return (
            self._campaign_contact_df.alias("cp_contact")
            .join(
                self._outbound_campaign_df.alias("ob_cp"),
                f.col("ob_cp.id") == f.col("cp_contact.campaign_id"),
                "inner",
            )
            .groupBy(f.col("ob_cp.company_id"), f.col("cp_contact.lead_id"))
            .agg(f.max(f.col("ob_cp.name")).alias("name"))
            .select(
                [
                    "name",
                    f.col("cp_contact.lead_id").alias("lead_id"),
                    f.col("ob_cp.company_id").alias("company_id"),
                ]
            )
        )

    def __collect_source_external(self):
        return (
            self._external_map_id_df.groupBy("company_id", "oid")
            .agg(
                f.max("ex_id_1").alias("ex_id_1"),
                f.max("ex_id_2").alias("ex_id_2"),
                f.max("ex_id_3").alias("ex_id_3"),
                f.max("ex_source").alias("ex_source"),
            )
            .select(
                [
                    "ex_id_1",
                    "ex_id_2",
                    "ex_id_3",
                    "ex_source",
                    "oid",
                    "company_id",
                ]
            )
        )

    def __collect_leads_sources(self):
        # Collect source event
        source_event_df = self.__collect_source_event()
        # Collect source widget type
        source_widget_type_df = self.__collect_source_widget_type()
        # Collect source campaign
        source_campaign_df = self.__collect_source_campaign()
        # Collect source external
        source_external_df = self.__collect_source_external()

        return (
            self._lead_df.alias("lead")
            .filter((f.col("lead.status") != LeadStatus.REMOVED) & (f.col("lead.name") != "") & (~f.col("lead.name").ilike("%Test%")) & (~f.col("lead.name").ilike("Unknow%")))
            .join(
                source_event_df.alias("source_event"),
                [
                    f.col("source_event.lead_id") == f.col("lead.id"),
                    f.col("source_event.company_id") == f.col("lead.company_id"),
                ],
                "left",
            )
            .join(
                source_widget_type_df.alias("source_widget_type"),
                [
                    f.col("source_widget_type.widget_type")
                    == f.when(
                        ((f.col("lead.source") == ApplySource.COMPANY_PAGE) & (f.col("lead.web_id") == 0)),
                        0,
                    ).otherwise(
                        f.when(
                            f.col("lead.source") == ApplySource.ERP_PAGE,
                            WebWidgetType.ERP_SITE,
                        ).otherwise(
                            f.when(
                                f.col("lead.source") == ApplySource.SCHEDULER_PAGE,
                                WebWidgetType.SCHEDULE,
                            ).otherwise(-1)
                        )
                    ),
                    f.col("source_widget_type.company_id") == f.col("lead.company_id"),
                ],
                "left",
            )
            .join(
                self._widget_df.alias("web_widget"),
                [
                    f.col("web_widget.id") == f.col("lead.web_id"),
                    f.col("web_widget.company_id") == f.col("lead.company_id"),
                ],
                "left",
            )
            .join(
                source_campaign_df.alias("source_campaign"),
                [
                    f.col("source_campaign.lead_id") == f.col("lead.id"),
                    f.col("source_campaign.company_id") == f.col("lead.company_id"),
                ],
                "left",
            )
            .join(
                self._authtool_user_df.alias("auth_user_add_by"),
                f.col("auth_user_add_by.id") == f.col("lead.added_by"),
                "left",
            )
            .join(
                self._company_user_df.alias("user_source_add_by"),
                [
                    f.col("user_source_add_by.user_id") == f.col("lead.added_by"),
                    f.col("user_source_add_by.company_id") == f.col("lead.company_id"),
                ],
                "left",
            )
            .join(
                self._contact_df.alias("contact_source_add_by"),
                f.col("user_source_add_by.user_id") == f.col("contact_source_add_by.user_id"),
                "left",
            )
            .join(
                self._employee_df.alias("source_referred_by"),
                [
                    f.col("source_referred_by.id") == f.col("lead.referred_by"),
                    f.col("source_referred_by.company_id") == f.col("lead.company_id"),
                ],
                "left",
            )
            .join(
                source_external_df.alias("source_external"),
                [
                    f.col("source_external.oid") == f.col("lead.long_id"),
                    f.col("source_external.company_id") == f.col("lead.company_id"),
                ],
                "left",
            )
            .join(
                self._lead_extra_data_df.alias("l_extra"),
                f.col("lead.id") == f.col("l_extra.lead_id"),
                "left",
            )
            .join(
                self._job_loc_df.alias("jl"),
                [
                    f.col("jl.id") == f.col("lead.job_loc_id"),
                    f.col("jl.is_deleted") == 0,
                ],
                "left",
            )
            .join(
                self._job_df.alias("jobs"),
                [
                    f.col("jobs.job_req_id") == f.col("l_extra.job_req_id"),
                    f.col("jobs.base_id") > 0,
                ],
                "left",
            )
            .join(
                self._job_family_df.alias("job_family"),
                f.col("jobs.job_family_id") == f.col("job_family.id"),
                "left",
            )
            .join(
                self._live_job_df.alias("live_job"),
                [
                    f.col("jobs.id") == f.col("live_job.job_id"),
                    f.col("jobs.job_req_id") == f.col("live_job.job_req_id"),
                    f.col("jl.id") == f.col("live_job.job_loc_id"),
                ],
                "left",
            )
        )

    def _transform_leads_multi_apply(self):
        """
        Refer SQL
        olivia_analytics/Apps/transform/sql/lead/lead_multi_apply.sql
        """
        self._lead_multi_apply_df = (
            self._lead_df.alias("lead")
            .join(
                self._candidate_profile_df.alias("profile"),
                f.col("profile.id") == f.col("lead.candidate_profile_id"),
                "inner",
            )
            .groupBy("lead.candidate_profile_id")
            .agg(
                f.count("lead.id").alias("total_app"),
                f.first("profile.public_id").alias("multi_apply_profile_id"),
            )
            .filter("total_app > 1")
            .select("lead.candidate_profile_id", "multi_apply_profile_id")
        )
        results_df = self._filter_candidate_df.join(self._lead_multi_apply_df, ["candidate_profile_id"], "left").select("lead_id", "multi_apply_profile_id")

        self._data_candidate_df = self._data_candidate_df.join(results_df, ["lead_id"], "left")

    def _transform_lead_logs_applied_job(self):
        """
        Refer SQL
        olivia_analytics/Apps/transform/sql/lead/lead_logs_applied_job.sql
        """
        # https://github.com/ParadoxAi/olivia-core/blob/05d79618b185f07d3448943e1ee517fd5acde6cd/src/ai_reports/init_sql/materialized_views/logs/mv_logs_applied_job.sql#L87
        MIN_CREATED_AT = "2021-07-01"

        # Collect max applied jobs
        max_aaj_df = (
            self._applied_job_df.filter(f.col("created_at") >= MIN_CREATED_AT)
            .groupBy("lead_id")
            .agg(
                f.max(f.when(f.col("is_valid") == 1, f.col("created_at")).otherwise(None).cast("timestamp")).alias("max_valid_created_at"),
                f.max(f.when(f.col("is_valid") == 0, f.col("created_at")).otherwise(None).cast("timestamp")).alias("max_invalid_created_at"),
                f.max(f.col("created_at")).alias("max_created_at"),
            )
            .select(
                "lead_id",
                "max_valid_created_at",
                "max_invalid_created_at",
                "max_created_at",
            )
        )

        # Collect applied jobs
        lead_applied_job_df = (
            self._applied_job_df.alias("aaj")
            .join(
                max_aaj_df.alias("max_aaj"),
                [
                    f.col("aaj.lead_id") == f.col("max_aaj.lead_id"),
                    f.when(
                        f.col("max_aaj.max_valid_created_at").isNotNull(),
                        f.col("aaj.is_valid") == 1,
                    ).otherwise(
                        f.when(
                            f.col("max_aaj.max_invalid_created_at").isNotNull(),
                            f.col("aaj.is_valid") == 0,
                        ).otherwise(False)
                    ),
                    f.coalesce(
                        f.col("max_aaj.max_valid_created_at"),
                        f.col("max_aaj.max_created_at"),
                    ).cast("timestamp")
                    == f.col("aaj.created_at"),
                ],
                "inner",
            )
            .select(
                "aaj.id",
                "aaj.lead_id",
                "aaj.job_req_id",
                "aaj.requisition_id",
                "aaj.job_title",
                "aaj.is_valid",
                "aaj.city",
                "aaj.state",
                "aaj.chat_to_apply_type",
                "aaj.created_at",
                "aaj.job_loc_id",
                "aaj.job_id",
                f.when(
                    f.get_json_object("aaj.extra_data", "$.is_internal_job_feed") == "true", True
                ).otherwise(False).alias("is_internal_job_feed")
            )
        )

        results_df = (
            self._lead_df.alias("l")
            .join(
                self._lead_extra_data_df.alias("l_extra"),
                f.col("l.id") == f.col("l_extra.lead_id"),
                "inner",
            )
            .join(
                self._job_df.alias("job"),
                (f.col("job.job_req_id") == f.col("l_extra.job_req_id")) & (f.col("job.base_id") > 0),
                "left",
            )
            .join(
                lead_applied_job_df.alias("applied_jobs"),
                f.col("applied_jobs.lead_id") == f.col("l.id"),
                "left",
            )
            .join(
                self._widget_df.alias("widget"),
                [
                    f.col("widget.id") == f.col("l.web_id"),
                    f.col("widget.company_id") == f.col("l.company_id"),
                ],
                "left",
            )
            .join(
                self._applied_job_extension.alias("applied_job_extension"),
                [
                    f.col("applied_job_extension.applied_job_id") == f.col("applied_jobs.id"),
                ],
                "left",
            )
            .select(
                f.col("l.id").alias("lead_id"),
                f.when(f.col("applied_jobs.is_valid") == 1, f.col("applied_jobs.id")).otherwise(None).alias("job_applied_valid_id"),
                f.when(
                    f.col("applied_jobs.is_valid") == 1, f.col("applied_jobs.requisition_id"),
                ).otherwise(None).alias("job_applied_requisition_id"),
                f.when(
                    f.col("applied_jobs.is_valid") == 0, None
                ).otherwise(
                    f.when(
                        f.col("applied_jobs.requisition_id").isNotNull() & (f.col("applied_jobs.requisition_id") != ""),
                        f.col("applied_jobs.requisition_id"),
                    ).otherwise(
                        f.when(
                            f.col("applied_jobs.job_req_id").isNotNull() & (f.col("applied_jobs.job_req_id") != ""),
                            f.col("applied_jobs.job_req_id"),
                        ).otherwise(
                            f.when(
                                f.col("job.job_req_id").isNotNull() & (f.col("job.job_req_id") != ""),
                                f.col("job.job_req_id"),
                            ).otherwise(f.col("l_extra.job_req_id"))
                        )
                    )
                ).alias("job_applied_log_job_requisition_id"),
                f.when(
                    f.col("applied_jobs.is_valid") == 0, None
                ).otherwise(
                    f.when(
                        f.col("applied_jobs.job_title").isNotNull() & (f.col("applied_jobs.job_title") != ""),
                        f.col("applied_jobs.job_title"),
                    ).otherwise(
                        f.when(
                            f.col("job.name").isNotNull() & (f.col("job.name") != ""),
                            f.col("job.name"),
                        ).otherwise(f.col("l_extra.job_title"))
                    )
                ).alias("job_applied_log_job_title"),
                f.when(f.col("applied_jobs.is_valid") == 1, f.col("applied_jobs.city")).otherwise(None).alias("job_applied_log_job_city"),
                f.when(f.col("applied_jobs.is_valid") == 1, f.col("applied_jobs.state")).otherwise(None).alias("job_applied_log_job_state"),
                f.when(
                    f.col("applied_jobs.is_valid") == 1,
                    f.col("applied_jobs.chat_to_apply_type"),
                ).otherwise(None).alias("job_applied_chat_to_apply_type"),
                f.when(
                    f.col("applied_jobs.is_valid") == 1,
                    f.col("applied_jobs.created_at"),
                ).otherwise(None).alias("job_applied_created_at"),
                f.when(
                    f.col("applied_jobs.is_valid") == 1,
                    f.col("applied_jobs.job_loc_id"),
                ).otherwise(None).alias("job_applied_job_loc_id"),
                f.when(f.col("applied_jobs.is_valid") == 1, f.col("applied_jobs.job_id")).otherwise(None).alias("job_applied_job_id"),
                f.when(f.col("applied_jobs.is_valid") == 1, f.col("applied_jobs.job_title")).otherwise(None).alias("job_applied_job_title"),
                f.col("applied_jobs.requisition_id").alias("job_applied_reapply_requisition_id"),
                f.col("applied_jobs.is_internal_job_feed").alias("is_internal_job_feed"),
                f.when(
                    f.col("l.referred_by") != CommonConst.DEFAULT_ID, ExtraApplySource.REFERRED_BY
                ).when(
                    f.col("l.apply_type") == ApplyType.EVENT, ExtraApplySource.EVENT
                ).when(
                    (f.col("l.web_id") != CommonConst.DEFAULT_ID) & (f.col("l.source").isin(ApplySource.COMPANY_PAGE, ApplySource.LANDING_SITE)),
                    f.when(f.col("widget.widget_type") == WebWidgetType.COMPANY_ADDITIONAL, ApplySource.LANDING_SITE).otherwise(ApplySource.COMPANY_PAGE)
                ).otherwise(f.col("l.source")).alias("cem_folder"),
                f.col("l.interview_type").alias("lead_interview_type").cast(types.IntegerType()),
                f.col("l.schedule_by").alias("lead_schedule_by"),
                f.col("l.interview_jobloc_id").alias("lead_interview_jobloc_id"),
                f.col("applied_job_extension.shorten_job_id").alias("job_applied_shorten_job_id")
            )
        )
        self._filter_candidate_df = self._filter_candidate_df.join(results_df, "lead_id", "left")

    def _transform_lead_itv_pre_attendee(self):
        """
        Refer SQL
        olivia_analytics/Apps/transform/sql/lead/lead_itv_pre_attendee.sql
        """

        pre_attendee_main_df = (
            self._itv_pre_attendee_df.alias("pre_attendee")
            .join(
                self._company_user_df.alias("company_user"),
                (f.col("pre_attendee.user_id") == f.col("company_user.user_id")) & (f.col("pre_attendee.company_id") == f.col("company_user.company_id")),
                "inner",
            )
            .join(
                self._contact_df.alias("contact"),
                f.col("company_user.user_id") == f.col("contact.user_id"),
                "inner",
            )
            .join(
                self._acl_role_df.alias("acl_role"),
                (f.col("acl_role.id") == f.col("company_user.acl_role_id")) & (f.col("acl_role.company_id") == f.col("company_user.company_id")),
                "left",
            )
            .select(
                [
                    "contact.name",
                    "pre_attendee.user_id",
                    "contact.phone_number",
                    "company_user.job_email",
                    "company_user.employee_id",
                    "company_user.role",
                    "company_user.type",
                    "company_user.tz_str",
                    "company_user.job_title",
                    f.col("acl_role.name").alias("acl_role_name"),
                    "pre_attendee.candidate_id",
                    "pre_attendee.company_id",
                ]
            )
        )

        results_df = (
            pre_attendee_main_df.alias("pre_attendee_main")
            .groupBy("pre_attendee_main.company_id", "pre_attendee_main.candidate_id")
            .agg(
                f.concat_ws(",", f.collect_set("pre_attendee_main.name")).alias("itv_pre_attendee_names"),
                f.concat_ws(",", f.collect_set("pre_attendee_main.job_email")).alias("itv_pre_attendee_emails"),
                f.concat_ws(",", f.collect_set("pre_attendee_main.employee_id")).alias("itv_pre_attendee_employee_ids"),
                f.concat_ws(",", f.collect_set("pre_attendee_main.phone_number")).alias("itv_pre_attendee_phones"),
                f.concat_ws(",", f.collect_set("pre_attendee_main.role")).alias("itv_roles"),
                f.concat_ws(",", f.collect_set("pre_attendee_main.acl_role_name")).alias("itv_acl_role_names"),
                f.concat_ws(",", f.collect_set("pre_attendee_main.user_id")).alias("itv_pre_attendee_user_ids"),
            )
            .select(
                [
                    "itv_pre_attendee_names",
                    "itv_pre_attendee_emails",
                    "itv_pre_attendee_employee_ids",
                    "itv_pre_attendee_phones",
                    "itv_roles",
                    "itv_acl_role_names",
                    "itv_pre_attendee_user_ids",
                    f.col("pre_attendee_main.candidate_id").alias("lead_id"),
                ]
            )
        )
        self._data_candidate_df = self._data_candidate_df.join(results_df, "lead_id", "left")

    def _transform_candidate_language(self):
        """
        Refer SQL
        olivia_analytics/Apps/transform/sql/lead/lead_language.sql
        """
        results_df = (
            self._lead_df.alias("lead")
            .join(
                self._language_df.alias("lang_detect"),
                f.col("lang_detect.code") == f.col("lead.detect_language_code"),
                "left"
            )
            .join(
                self._language_df.alias("lang"),
                f.col("lang.code") == f.col("lead.language_code"),
                "left"
            )
            .select([
                f.col("lead.id").alias("lead_id"),
                f.when(f.col("lang_detect.name").isNotNull(), f.col("lang_detect.name"))
                .otherwise(
                    f.when(f.col("lang.name").isNotNull(), f.col("lang.name"))
                    .otherwise(
                        f.when(f.col("lead.detect_language_code").isNotNull(), f.col("lead.detect_language_code"))
                        .otherwise(f.col("lead.language_code"))
                    )
                ).alias("candidate_language")
            ])
        )
        self._data_candidate_df = self._data_candidate_df.join(results_df, "lead_id", "left")

    def _transform_applied_job_extra_data(self):
        """
        Refer SQL
        olivia_analytics/Apps/transform/sql/capture/candidate_specific.sql
        """

        results_df = self._filter_candidate_df.alias("filter_candidate").join(
            self._applied_job_extra_data.alias("applied_job_extra"),
            [
                f.col("applied_job_extra.applied_job_id") == f.col("filter_candidate.latest_apply_job_id"),
                f.col("applied_job_extra.system_attribute_key_name") == "cf_custom_org",
            ],
            "left"
        ).select([
            f.col("filter_candidate.lead_id").alias("lead_id"),
            f.col("applied_job_extra.custom_field_value").alias("job_applied_extra_org")
        ])
        self._filter_candidate_df = self._filter_candidate_df.join(results_df, "lead_id", "left")

CandidateTransformJob().transform()
