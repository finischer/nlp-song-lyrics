from collections import Counter


def sample(data, balanced=True, prop=0.2):
    if balanced:
        # compute the sorted decreasing parties frequencies
        decade_frequencies = Counter(data["decade"]).most_common()
        print(decade_frequencies)

        # retrieve the under represented class
        nb_under_class = decade_frequencies[-1][1]

        # Return a random sample of items from each party following the under sampled number of class
        sample_df = data.groupby("decade").sample(
            n=int(prop * nb_under_class), random_state=20
        )
    else:
        # create sample df 1/3 of the actual loaded data
        sample_df = df.sample(frac=prop)
    return sample_df
