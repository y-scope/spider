//! A round's events bound to their outcomes in the round's plan.

use spider_core::types::id::JobId;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::cache::task::PreparedChild;
use crate::cache::tree::Lane;
use crate::cache::tree::Pending;
use crate::cache::tree::RoundOutcome;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventKind;
use crate::db::DbError;

/// The events of a round bound to their outcomes in the round's plan, one outcome per event, in
/// order.
///
/// Every event whose outcome registers children is a [`TreeEventKind::RegularTasksDone`], so the
/// walks over the children of the round's registering events never meet another kind:
///
/// * [`Self::new`], the only constructor, checks it.
/// * Taking an event's children out keeps the event's kind.
/// * The only method that changes an event's kind consumes the round.
///
/// # Type Parameters
///
/// * `'in_flight_lifetime` - The lifetime of the borrowed events of the round.
pub(super) struct PlannedRound<'in_flight_lifetime> {
    in_flight: &'in_flight_lifetime mut Vec<Pending>,
    outcomes: Vec<RoundOutcome>,
    has_child_registration: bool,
}

impl<'in_flight_lifetime> PlannedRound<'in_flight_lifetime> {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The events of `in_flight` bound to `outcomes`, one outcome per event in order, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::RoundOutcomeCountMismatch`] if `outcomes` does not have exactly one
    ///   outcome per event of `in_flight`.
    /// * [`InternalError::RegistrationMismatch`] if an outcome registers children of an event that
    ///   is not a [`TreeEventKind::RegularTasksDone`].
    pub(super) fn new(
        in_flight: &'in_flight_lifetime mut Vec<Pending>,
        outcomes: Vec<RoundOutcome>,
    ) -> Result<Self, InternalError> {
        if outcomes.len() != in_flight.len() {
            return Err(InternalError::RoundOutcomeCountMismatch {
                num_outcomes: outcomes.len(),
                num_events: in_flight.len(),
            });
        }
        let has_child_registration = outcomes.iter().any(RoundOutcome::has_child_registration);
        let round = Self {
            in_flight,
            outcomes,
            has_child_registration,
        };
        if let Some(job_id) = round
            .registering_events()
            .find(|event| !matches!(event.kind, TreeEventKind::RegularTasksDone { .. }))
            .map(|event| event.job_id)
        {
            return Err(InternalError::RegistrationMismatch(job_id));
        }
        Ok(round)
    }

    /// # Returns
    ///
    /// Whether any event of the round registers children.
    pub(super) const fn has_child_registration(&self) -> bool {
        self.has_child_registration
    }

    /// # Returns
    ///
    /// The ID of each registering event's job and the children that the event carries, in the order
    /// of the round's events.
    pub(super) fn registered_child_sets(&self) -> impl Iterator<Item = (JobId, &[PreparedChild])> {
        self.registered_children()
            .map(|(job_id, children)| (job_id, children.as_slice()))
    }

    /// # Returns
    ///
    /// The number of children that the round's registering events carry.
    pub(super) fn num_registered_children(&self) -> usize {
        self.registered_children()
            .map(|(_, children)| children.len())
            .sum()
    }

    /// Takes the children out of the round's registering events, leaving each with none.
    ///
    /// # Returns
    ///
    /// The ID of each registering event's job and the children taken out of the event, in the order
    /// of [`Self::registered_child_sets`].
    pub(super) fn take_registered_child_sets(
        &mut self,
    ) -> impl Iterator<Item = (JobId, Vec<PreparedChild>)> {
        self.registered_children_mut()
            .map(|(job_id, children)| (job_id, std::mem::take(children)))
    }

    /// Turns an error of the round's write that one event of the round causes into a failure, on
    /// the regular lane, of that event's job:
    ///
    /// * A [`DbError::OutputsTooLarge`] fails the job whose outputs the database rejects: the
    ///   round's planned [`TreeEventKind::RegularTasksDone`] event of that job becomes that
    ///   failure.
    /// * A [`DbError::ChildTooLarge`] fails the child's parent if one of its events registers
    ///   children: that registering event becomes that failure.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`CacheError`] converted from `error` if it is none of the errors above.
    pub(super) fn fail_job_with_oversized_payload(
        mut self,
        error: DbError,
    ) -> Result<(), CacheError> {
        let Some(event) = (match &error {
            DbError::OutputsTooLarge { job_id, .. } => self
                .in_flight
                .iter_mut()
                .zip(&self.outcomes)
                .find_map(|(pending, outcome)| {
                    (matches!(outcome, RoundOutcome::Planned { .. })
                        && pending.event.job_id == *job_id
                        && matches!(pending.event.kind, TreeEventKind::RegularTasksDone { .. }))
                    .then_some(&mut pending.event)
                }),
            DbError::ChildTooLarge { parent_id, .. } => self
                .registering_events_mut()
                .find(|event| event.job_id == *parent_id),
            _ => None,
        }) else {
            return Err(error.into());
        };
        tracing::warn!(
            job_id = ? event.job_id,
            error = % error,
            "The database rejected the payload of a job's event. Failing the job."
        );
        event.kind = TreeEventKind::TerminalFailure {
            lane: Lane::Regular,
            message: error.to_string(),
        };
        Ok(())
    }

    /// Replies to each planned event of the round with its planned outcome, leaving only the
    /// deferred events, in order, in the events that the round borrows.
    pub(super) fn answer(self) {
        // Every deferred event goes back into `in_flight` for the committer to requeue. The loop
        // neither panics nor awaits, so no reply is dropped before the committer's context guard
        // acts.
        for (pending, outcome) in std::mem::take(self.in_flight)
            .into_iter()
            .zip(self.outcomes)
        {
            match outcome {
                RoundOutcome::Planned { outcome, .. } => {
                    // The event's waiter may have dropped its future, which is not an error of the
                    // committer.
                    let _ = pending.reply.send(Ok(outcome));
                }
                RoundOutcome::Deferred => self.in_flight.push(pending),
            }
        }
    }

    /// # Returns
    ///
    /// The round's events whose outcomes register children, in order.
    fn registering_events(&self) -> impl Iterator<Item = &TreeEvent> {
        self.in_flight
            .iter()
            .zip(&self.outcomes)
            .filter(|(_, outcome)| outcome.has_child_registration())
            .map(|(pending, _)| &pending.event)
    }

    /// # Returns
    ///
    /// The round's events whose outcomes register children, mutably and in order.
    fn registering_events_mut(&mut self) -> impl Iterator<Item = &mut TreeEvent> {
        self.in_flight
            .iter_mut()
            .zip(&self.outcomes)
            .filter(|(_, outcome)| outcome.has_child_registration())
            .map(|(pending, _)| &mut pending.event)
    }

    /// # Returns
    ///
    /// The ID of each registering event's job and the children that the event carries, in the order
    /// of [`Self::registering_events`].
    ///
    /// # Panics
    ///
    /// Panics if a registering event is not a [`TreeEventKind::RegularTasksDone`], which
    /// [`Self::new`] rules out.
    fn registered_children(&self) -> impl Iterator<Item = (JobId, &Vec<PreparedChild>)> {
        self.registering_events().map(|event| match &event.kind {
            TreeEventKind::RegularTasksDone { children, .. } => (event.job_id, children),
            TreeEventKind::Start { .. }
            | TreeEventKind::CommitDone
            | TreeEventKind::CleanupDone
            | TreeEventKind::TerminalFailure { .. }
            | TreeEventKind::CancelRoot => unreachable!(
                "a registering event should be a `RegularTasksDone`, as `PlannedRound::new` checks"
            ),
        })
    }

    /// # Returns
    ///
    /// The ID of each registering event's job and the children that the event carries, mutably and
    /// in the order of [`Self::registering_events_mut`].
    ///
    /// # Panics
    ///
    /// Panics if a registering event is not a [`TreeEventKind::RegularTasksDone`], which
    /// [`Self::new`] rules out.
    fn registered_children_mut(
        &mut self,
    ) -> impl Iterator<Item = (JobId, &mut Vec<PreparedChild>)> {
        self.registering_events_mut()
            .map(|event| match &mut event.kind {
                TreeEventKind::RegularTasksDone { children, .. } => (event.job_id, children),
                TreeEventKind::Start { .. }
                | TreeEventKind::CommitDone
                | TreeEventKind::CleanupDone
                | TreeEventKind::TerminalFailure { .. }
                | TreeEventKind::CancelRoot => unreachable!(
                    "a registering event should be a `RegularTasksDone`, as `PlannedRound::new` \
                     checks"
                ),
            })
    }
}

#[cfg(test)]
mod tests {
    use spider_core::job::JobState;
    use tokio::sync::oneshot;

    use super::*;
    use crate::cache::error::StaleStateError;
    use crate::cache::tree::TreeEvent;
    use crate::cache::tree::TreeEventOutcome;
    use crate::state::test_utils::MOCK_MAX_CHILD_SIZE;
    use crate::state::test_utils::MOCK_MAX_OUTPUTS_SIZE;
    use crate::state::test_utils::create_test_prepared_children;
    use crate::state::tree_committer::test_utils::ROOT_ID;
    use crate::state::tree_committer::test_utils::payload_rejected;
    use crate::state::tree_committer::test_utils::pending;
    use crate::state::tree_committer::test_utils::registration;
    use crate::state::tree_committer::test_utils::regular_tasks_done;
    use crate::state::tree_committer::test_utils::start;

    #[test]
    fn a_round_needs_one_outcome_per_event_and_completions_for_its_registrations() {
        let mut in_flight = vec![pending(ROOT_ID.get(), start())];

        let error = PlannedRound::new(&mut in_flight, Vec::new()).err();
        assert!(
            matches!(
                error,
                Some(InternalError::RoundOutcomeCountMismatch {
                    num_outcomes: 0,
                    num_events: 1,
                })
            ),
            "a round without an outcome for its event gave {error:?}"
        );

        let error = PlannedRound::new(
            &mut in_flight,
            vec![RoundOutcome::Planned {
                outcome: TreeEventOutcome::Applied(JobState::Running),
                has_child_registration: true,
            }],
        )
        .err();
        assert!(
            matches!(
                error,
                Some(InternalError::RegistrationMismatch(job_id)) if job_id == ROOT_ID
            ),
            "a round registering the children of a start gave {error:?}"
        );
    }

    #[test]
    fn answering_a_round_replies_to_its_planned_events_and_keeps_the_deferred_ones_in_order()
    -> anyhow::Result<()> {
        let (reply_sender, mut reply_receiver) = oneshot::channel();
        let mut in_flight = vec![
            pending(ROOT_ID.get() + 1, TreeEventKind::CommitDone),
            Pending {
                event: TreeEvent {
                    job_id: ROOT_ID,
                    kind: start(),
                },
                reply: reply_sender,
            },
            pending(ROOT_ID.get() + 2, TreeEventKind::CleanupDone),
        ];

        PlannedRound::new(
            &mut in_flight,
            vec![
                RoundOutcome::Deferred,
                RoundOutcome::Planned {
                    outcome: TreeEventOutcome::Applied(JobState::Running),
                    has_child_registration: false,
                },
                RoundOutcome::Deferred,
            ],
        )?
        .answer();
        let reply = reply_receiver.try_recv()?;
        assert!(
            matches!(reply, Ok(TreeEventOutcome::Applied(JobState::Running))),
            "the planned event got {reply:?}"
        );
        assert_eq!(
            in_flight
                .iter()
                .map(|pending| pending.event.job_id)
                .collect::<Vec<_>>(),
            [
                JobId::from(ROOT_ID.get() + 1),
                JobId::from(ROOT_ID.get() + 2)
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_round_walks_the_child_sets_of_only_its_registering_events_in_order()
    -> anyhow::Result<()> {
        let (mut in_flight, outcomes) = round_with_registering_and_other_registrations().await;

        let round = PlannedRound::new(&mut in_flight, outcomes)?;
        assert_eq!(
            round
                .registered_child_sets()
                .map(|(job_id, children)| (job_id.get(), children.len()))
                .collect::<Vec<_>>(),
            [(ROOT_ID.get() + 1, 2), (ROOT_ID.get() + 5, 3)]
        );
        assert_eq!(round.num_registered_children(), 5);
        Ok(())
    }

    #[tokio::test]
    async fn taking_a_rounds_child_sets_empties_only_its_registering_events() -> anyhow::Result<()>
    {
        let (mut in_flight, outcomes) = round_with_registering_and_other_registrations().await;

        let mut round = PlannedRound::new(&mut in_flight, outcomes)?;
        assert_eq!(
            round
                .take_registered_child_sets()
                .map(|(job_id, children)| (job_id.get(), children.len()))
                .collect::<Vec<_>>(),
            [(ROOT_ID.get() + 1, 2), (ROOT_ID.get() + 5, 3)]
        );
        drop(round);
        assert_eq!(
            in_flight
                .iter()
                .map(|pending| (
                    pending.event.job_id.get(),
                    match &pending.event.kind {
                        TreeEventKind::RegularTasksDone { children, .. } => Some(children.len()),
                        _ => None,
                    }
                ))
                .collect::<Vec<_>>(),
            [
                (ROOT_ID.get() + 1, Some(0)),
                (ROOT_ID.get() + 2, Some(1)),
                (ROOT_ID.get(), None),
                (ROOT_ID.get() + 4, Some(1)),
                (ROOT_ID.get() + 5, Some(0)),
            ]
        );
        Ok(())
    }

    #[test]
    fn rejected_outputs_fail_only_the_planned_completion_of_their_job() -> anyhow::Result<()> {
        let mut in_flight = vec![
            pending(ROOT_ID.get() + 1, regular_tasks_done()),
            pending(ROOT_ID.get(), TreeEventKind::CommitDone),
            pending(ROOT_ID.get(), regular_tasks_done()),
        ];
        let outcomes = vec![
            RoundOutcome::Planned {
                outcome: TreeEventOutcome::Untracked,
                has_child_registration: false,
            },
            RoundOutcome::Planned {
                outcome: TreeEventOutcome::Stale(StaleStateError::JobNoLongerCommitReady),
                has_child_registration: false,
            },
            RoundOutcome::Planned {
                outcome: TreeEventOutcome::Applied(JobState::Succeeded),
                has_child_registration: false,
            },
        ];
        let error = DbError::OutputsTooLarge {
            job_id: ROOT_ID,
            size: MOCK_MAX_OUTPUTS_SIZE + 1,
            limit: MOCK_MAX_OUTPUTS_SIZE,
        };
        let expected_message = error.to_string();

        PlannedRound::new(&mut in_flight, outcomes)?.fail_job_with_oversized_payload(error)?;
        let [other_job_completion, stale_commit_done, completion] = in_flight.as_slice() else {
            panic!("the round should keep its three events");
        };
        assert!(
            matches!(
                other_job_completion.event.kind,
                TreeEventKind::RegularTasksDone { .. }
            ),
            "the completion of another job should be kept"
        );
        assert!(
            matches!(stale_commit_done.event.kind, TreeEventKind::CommitDone),
            "a stale event of the job should be kept"
        );
        assert!(
            matches!(
                &completion.event.kind,
                TreeEventKind::TerminalFailure {
                    lane: Lane::Regular,
                    message,
                } if *message == expected_message
            ),
            "the job's planned completion should become its failure"
        );
        Ok(())
    }

    #[test]
    fn rejected_outputs_do_not_fail_a_deferred_completion_of_their_job() -> anyhow::Result<()> {
        let mut in_flight = vec![
            pending(ROOT_ID.get(), regular_tasks_done()),
            pending(ROOT_ID.get(), regular_tasks_done()),
        ];
        let outcomes = vec![
            RoundOutcome::Deferred,
            RoundOutcome::Planned {
                outcome: TreeEventOutcome::Applied(JobState::Succeeded),
                has_child_registration: false,
            },
        ];
        let error = DbError::OutputsTooLarge {
            job_id: ROOT_ID,
            size: MOCK_MAX_OUTPUTS_SIZE + 1,
            limit: MOCK_MAX_OUTPUTS_SIZE,
        };

        PlannedRound::new(&mut in_flight, outcomes)?.fail_job_with_oversized_payload(error)?;
        let [deferred_completion, planned_completion] = in_flight.as_slice() else {
            panic!("the round should keep its two events");
        };
        assert!(
            matches!(
                deferred_completion.event.kind,
                TreeEventKind::RegularTasksDone { .. }
            ),
            "the job's deferred completion should be kept"
        );
        assert!(
            matches!(
                planned_completion.event.kind,
                TreeEventKind::TerminalFailure {
                    lane: Lane::Regular,
                    ..
                }
            ),
            "the job's planned completion should become its failure"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_child_too_large_fails_only_its_parents_registering_event() -> anyhow::Result<()> {
        const OTHER_PARENT_ID: JobId = JobId::from(ROOT_ID.get() + 1);
        let children = create_test_prepared_children(1).await;
        let other_children = create_test_prepared_children(2).await;
        let mut in_flight = vec![
            pending(OTHER_PARENT_ID.get(), registration(other_children)),
            pending(ROOT_ID.get(), registration(children)),
            pending(ROOT_ID.get(), start()),
        ];
        let outcomes = |has_child_registration| {
            [
                RoundOutcome::Planned {
                    outcome: TreeEventOutcome::Applied(JobState::BlockingOnChildren),
                    has_child_registration,
                },
                RoundOutcome::Planned {
                    outcome: TreeEventOutcome::Applied(JobState::BlockingOnChildren),
                    has_child_registration,
                },
                RoundOutcome::Planned {
                    outcome: TreeEventOutcome::Stale(StaleStateError::JobAlreadyStarted),
                    has_child_registration: false,
                },
            ]
        };
        let child_too_large = |parent_id| DbError::ChildTooLarge {
            parent_id,
            size: MOCK_MAX_CHILD_SIZE + 1,
            limit: MOCK_MAX_CHILD_SIZE,
        };

        for (error, has_child_registration) in [
            (child_too_large(ROOT_ID), false),
            (child_too_large(JobId::from(ROOT_ID.get() + 2)), true),
            (payload_rejected(), true),
            (DbError::Sql(sqlx::Error::PoolTimedOut), true),
        ] {
            let result =
                PlannedRound::new(&mut in_flight, outcomes(has_child_registration).into())?
                    .fail_job_with_oversized_payload(error);
            assert!(
                matches!(result, Err(CacheError::Internal(InternalError::Db(_)))),
                "a write error that no registering event causes gave {result:?}"
            );
        }
        assert!(
            in_flight.iter().all(|pending| !matches!(
                pending.event.kind,
                TreeEventKind::TerminalFailure { .. }
            )),
            "an error that no event causes should fail no event"
        );

        let error = child_too_large(ROOT_ID);
        let expected_message = error.to_string();
        PlannedRound::new(&mut in_flight, outcomes(true).into())?
            .fail_job_with_oversized_payload(error)?;
        let [other_registration, failed_registration, stale_start] = in_flight.as_slice() else {
            panic!("the round should keep its three events");
        };
        assert!(
            matches!(
                other_registration.event.kind,
                TreeEventKind::RegularTasksDone { .. }
            ),
            "the registration of another parent should be kept"
        );
        assert!(
            matches!(
                &failed_registration.event.kind,
                TreeEventKind::TerminalFailure {
                    lane: Lane::Regular,
                    message,
                } if *message == expected_message
            ),
            "the registering event should become its job's failure"
        );
        assert!(
            matches!(stale_start.event.kind, TreeEventKind::Start { .. }),
            "a stale event of the job should be kept"
        );
        Ok(())
    }

    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The events of a round, in order:
    ///   * A registration of two children, which the round registers.
    ///   * A registration of one child, which the round defers.
    ///   * A start.
    ///   * A stale registration of one child.
    ///   * A registration of three children, which the round registers.
    /// * The round's outcomes, one per event in order.
    async fn round_with_registering_and_other_registrations() -> (Vec<Pending>, Vec<RoundOutcome>) {
        let in_flight = vec![
            pending(
                ROOT_ID.get() + 1,
                registration(create_test_prepared_children(2).await),
            ),
            pending(
                ROOT_ID.get() + 2,
                registration(create_test_prepared_children(1).await),
            ),
            pending(ROOT_ID.get(), start()),
            pending(
                ROOT_ID.get() + 4,
                registration(create_test_prepared_children(1).await),
            ),
            pending(
                ROOT_ID.get() + 5,
                registration(create_test_prepared_children(3).await),
            ),
        ];
        let outcomes = vec![
            RoundOutcome::Planned {
                outcome: TreeEventOutcome::Applied(JobState::BlockingOnChildren),
                has_child_registration: true,
            },
            RoundOutcome::Deferred,
            RoundOutcome::Planned {
                outcome: TreeEventOutcome::Applied(JobState::Running),
                has_child_registration: false,
            },
            RoundOutcome::Planned {
                outcome: TreeEventOutcome::Stale(StaleStateError::JobNoLongerRunning),
                has_child_registration: false,
            },
            RoundOutcome::Planned {
                outcome: TreeEventOutcome::Applied(JobState::BlockingOnChildren),
                has_child_registration: true,
            },
        ];
        (in_flight, outcomes)
    }
}
