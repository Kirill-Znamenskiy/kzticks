package kzticks

import (
	"context"
	"fmt"
	"github.com/Kirill-Znamenskiy/kzlogger/callers"
	"github.com/Kirill-Znamenskiy/kzlogger/lg"
	"github.com/Kirill-Znamenskiy/kzlogger/lga"
	"github.com/Kirill-Znamenskiy/kzlogger/lge"
	"github.com/Kirill-Znamenskiy/kzutils"
	"github.com/sourcegraph/conc/panics"
	"time"
)

type Ctx = context.Context

type RunTickFunc func(Ctx, string) (string, error)

func RunTicks(ctx Ctx,
	lgr lg.LoggerInterface,
	daemonName string,
	daemonTicksName string,
	toRunTickFunc RunTickFunc,
	maxCntOfTicks int,
	wrkTickerInterval time.Duration,
	minToWaitInterval, maxToWaitInterval time.Duration,
) {

	ind := 0
	logPrefix := fmt.Sprintf("%s (%s:%d)", daemonName, daemonTicksName, ind)
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	lgr.Info(ctx, logPrefix+" Now I'm starting ticks!",
		lga.Int("maxCntOfTicks", maxCntOfTicks),
		lga.String("wrkTickerInterval", wrkTickerInterval.String()),
		lga.String("minToWaitInterval", minToWaitInterval.String()),
		lga.String("maxToWaitInterval", maxToWaitInterval.String()),
	)
	for {
		ind++
		logPrefix = fmt.Sprintf("%s (%s:%d)", daemonName, daemonTicksName, ind)
		if maxCntOfTicks >= 0 && ind > maxCntOfTicks {
			lgr.Info(ctx, logPrefix+" Now I'm going to finish with ticks, because (maxCntOfTicks >= 0 && ind > maxCntOfTicks)!",
				lga.Int("ind", ind),
				lga.Int("maxCntOfTicks", maxCntOfTicks),
			)
			break
		}

		isNeedToBreak := false
		select {
		case <-ctx.Done():
			lgr.Info(ctx, logPrefix+" Now I'm going to finish with ticks, because context is done!")
			isNeedToBreak = true
			break
		case <-timer.C:
		}
		if isNeedToBreak {
			break
		}

		tickStartedAt := time.Now()

		var (
			err         error
			tickResults string
		)
		recoveredPanic := panics.Try(func() {
			tickResults, err = toRunTickFunc(ctx, logPrefix)
		})
		tickFinishedAt := time.Now()
		tickDuration := tickFinishedAt.Sub(tickStartedAt)
		tickDuration = kzutils.RoundDuration(tickDuration, time.Second, 2)

		if recoveredPanic != nil {
			err = lge.NewErr(fmt.Sprintf("panic: %v", recoveredPanic.Value),
				lga.RuntimeFrames("stack", (*callers.Callers)(&recoveredPanic.Callers).FramesSlice()),
			)
			lgr.Error(ctx, logPrefix+" Tick finished with panic!",
				lga.Any("panic", err),
				lga.Time("tickStartedAt", tickStartedAt),
				lga.Time("tickFinishedAt", tickFinishedAt),
				lga.String("tickDuration", tickDuration.String()),
			)
		} else if err != nil {
			lgr.Error(ctx, logPrefix+" Tick finished with error!",
				lga.Error(err),
				lga.Time("tickStartedAt", tickStartedAt),
				lga.Time("tickFinishedAt", tickFinishedAt),
				lga.String("tickDuration", tickDuration.String()),
			)
		} else {
			lgr.Info(ctx, logPrefix+" Tick finished successfully with some results!",
				lga.String("tickResults", tickResults),
				lga.Time("tickStartedAt", tickStartedAt),
				lga.Time("tickFinishedAt", tickFinishedAt),
				lga.String("tickDuration", tickDuration.String()),
			)
		}

		toWaitDuration1 := wrkTickerInterval - tickDuration
		toWaitDuration1 = kzutils.RoundDuration(toWaitDuration1, time.Second, 2)
		toWaitDuration2 := toWaitDuration1
		if minToWaitInterval > 0 && toWaitDuration2 < minToWaitInterval {
			toWaitDuration2 = minToWaitInterval
		}
		if maxToWaitInterval > 0 && toWaitDuration2 > maxToWaitInterval {
			toWaitDuration2 = maxToWaitInterval
		}
		toWaitDuration2 = kzutils.RoundDuration(toWaitDuration2, time.Second, 2)

		lgr.Info(ctx, fmt.Sprintf(logPrefix+" Now I'm going to sleep for: %s - %s = %s => %s",
			wrkTickerInterval, tickDuration, toWaitDuration1, toWaitDuration2,
		))

		timer.Reset(toWaitDuration2)
	}

	timer.Stop()
	lgr.Info(ctx, logPrefix+" Now I'm finishing with ticks :((")
	return

}
