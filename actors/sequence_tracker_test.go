package actors_test

import (
	. "github.com/kphelps/actors/actors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SequenceTracker", func() {
	var st SequenceTracker

	BeforeEach(func() {
		st = NewSequenceTracker(cassandraSession)
	})

	Context("Getting a sequence before an update", func() {
		It("Returns 0", func() {
			foundSequenceID, err := st.GetSequenceID("name")
			Expect(err).NotTo(HaveOccurred())
			Expect(foundSequenceID).To(Equal(uint64(0)))
		})
	})

	Context("Updating a sequence", func() {
		sequenceName := "name"

		Context("Sequence doesn't exist", func() {
			It("Creates a new sequence", func() {
				sequenceID := uint64(10)
				err := st.UpdateSequence(sequenceName, sequenceID).Execute(cassandraSession)
				Expect(err).NotTo(HaveOccurred())
				foundSequenceID, err := st.GetSequenceID(sequenceName)
				Expect(err).NotTo(HaveOccurred())
				Expect(foundSequenceID).To(Equal(sequenceID))
			})
		})

		Context("SequenceID already exists", func() {
			It("Updates the sequence", func() {
				err := st.UpdateSequence(sequenceName, 1).Execute(cassandraSession)
				Expect(err).NotTo(HaveOccurred())
				err = st.UpdateSequence(sequenceName, uint64(20)).Execute(cassandraSession)
				Expect(err).NotTo(HaveOccurred())
				foundSequenceID, err := st.GetSequenceID(sequenceName)
				Expect(err).NotTo(HaveOccurred())
				Expect(foundSequenceID).To(Equal(uint64(20)))
			})
		})
	})
})
