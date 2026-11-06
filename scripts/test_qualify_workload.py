import unittest

from qualify_workload import HOUR, INTERVAL, batches, workload_horizon


class WorkloadFixtureTests(unittest.TestCase):
    def test_sparse_history_and_dense_cohort_have_no_extra_series(self):
        horizon = workload_horizon(100 * HOUR + 123456, 24, 2)
        fixture = list(batches(3, 13, horizon, 5))
        self.assertEqual([len(batch) for batch in fixture], [5, 5, 3])
        series = [item for batch in fixture for item in batch]
        self.assertEqual(len({labels['instance'] for labels, _ in series}), 13)
        self.assertEqual(len(series[0][1]), 24 * 120)
        self.assertEqual({len(samples) for _, samples in series[1:]}, {2 * 120})
        self.assertEqual(series[0][1][0][0], horizon['start_ms'])
        for labels, samples in series:
            self.assertEqual(samples[-1][0], horizon['boundary_ms'] - INTERVAL)
            self.assertEqual({b[0] - a[0] for a, b in zip(samples, samples[1:])}, {INTERVAL})
            self.assertEqual(labels['__name__'], 'qualify_load')

    def test_complete_history_mode_is_dense_for_every_series(self):
        horizon = workload_horizon(100 * HOUR, 24, 24)
        for batch in batches(0, 3, horizon, 2):
            for _, samples in batch:
                self.assertEqual(len(samples), 24 * 120)
                self.assertEqual(samples[0][0], horizon['start_ms'])

    def test_cutoff_is_fixed_across_the_next_utc_hour(self):
        clock = 100 * HOUR + HOUR - 1000
        horizon = workload_horizon(clock, 24, 2)
        for elapsed in (0, 1001, HOUR // 2 - 1):
            effective = (clock + elapsed - horizon['archive_age_ms']) // HOUR * HOUR
            self.assertEqual(effective, horizon['boundary_ms'])
        self.assertEqual((horizon['next_eligible_at_ms'] - horizon['archive_age_ms']) // HOUR * HOUR,
                         horizon['boundary_ms'] + HOUR)


if __name__ == '__main__':
    unittest.main()
