package bench

func BankLikeGenerator(versions int64, scale float64) StoreParams {
	return StoreParams{
		StoreKey:         "bank",
		KeyMean:          56,
		KeyStdDev:        3,
		ValueMean:        100,
		ValueStdDev:      1200,
		InitialSize:      35_000,
		FinalSize:        int(float64(2_200_200) * scale),
		Versions:         versions,
		ChangePerVersion: 1840,
		DeleteFraction:   0.25,
	}
}

func LockupLikeGenerator(versions int64, scale float64) StoreParams {
	return StoreParams{
		StoreKey:         "lockup",
		KeyMean:          56,
		KeyStdDev:        3,
		ValueMean:        1936,
		ValueStdDev:      29261,
		InitialSize:      35_000,
		FinalSize:        int(float64(2_600_200) * scale),
		Versions:         versions,
		ChangePerVersion: 363,
		DeleteFraction:   0.29,
	}
}

func StakingLikeGenerator(versions int64, scale float64) StoreParams {
	return StoreParams{
		StoreKey:         "staking",
		KeyMean:          24,
		KeyStdDev:        2,
		ValueMean:        12263,
		ValueStdDev:      22967,
		InitialSize:      35_000,
		FinalSize:        int(float64(1_600_696) * scale),
		Versions:         versions,
		ChangePerVersion: 305,
		DeleteFraction:   0.25,
	}
}

func OsmoLikeGenerators(scale float64) []StoreParams {
	initialSize := int(20_000_000 * scale)
	finalSize := int(1.5 * float64(initialSize))
	var versions int64 = 1_000_000
	bankGen := BankLikeGenerator(versions, scale)
	bankGen.InitialSize = initialSize
	bankGen.FinalSize = finalSize
	bankGen2 := BankLikeGenerator(versions, scale)
	bankGen2.StoreKey = "bank2"
	bankGen2.InitialSize = initialSize
	bankGen2.FinalSize = finalSize

	return []StoreParams{
		bankGen,
		bankGen2,
	}
}

func SampleGenerators(versions int64, scale float64) []StoreParams {
	if versions <= 0 {
		versions = 20
	}
	if scale <= 0 {
		scale = 1
	}

	v := int(versions)
	if v < 1 {
		v = 1
	}

	initialSize := int(200 * scale)
	if initialSize < 10 {
		initialSize = 10
	}

	bankChanges := int(50 * scale)
	if bankChanges < 1 {
		bankChanges = 1
	}
	stakingChanges := int(10 * scale)
	if stakingChanges < 1 {
		stakingChanges = 1
	}
	lockupChanges := int(10 * scale)
	if lockupChanges < 1 {
		lockupChanges = 1
	}

	return []StoreParams{
		{
			StoreKey:         "bank",
			KeyMean:          56,
			KeyStdDev:        3,
			ValueMean:        100,
			ValueStdDev:      1200,
			InitialSize:      initialSize,
			FinalSize:        initialSize + bankChanges*v,
			Versions:         versions,
			ChangePerVersion: bankChanges,
			DeleteFraction:   0.25,
		},
		{
			StoreKey:         "staking",
			KeyMean:          24,
			KeyStdDev:        2,
			ValueMean:        12_263,
			ValueStdDev:      22_967,
			InitialSize:      initialSize,
			FinalSize:        initialSize + stakingChanges*v,
			Versions:         versions,
			ChangePerVersion: stakingChanges,
			DeleteFraction:   0.25,
		},
		{
			StoreKey:         "lockup",
			KeyMean:          56,
			KeyStdDev:        3,
			ValueMean:        1_936,
			ValueStdDev:      29_261,
			InitialSize:      initialSize,
			FinalSize:        initialSize + lockupChanges*v,
			Versions:         versions,
			ChangePerVersion: lockupChanges,
			DeleteFraction:   0.29,
		},
	}
}
