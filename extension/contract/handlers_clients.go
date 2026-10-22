package contract

import (
	"context"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/tether/topic"
)

// retainedFilterDefault lists every retained message of the tenant except
// its $SYS topics, which a leading wildcard never matches.
const retainedFilterDefault = "#"

func clientsListHandler(sp ServicesProvider) handlerFunc[TenantRef, ClientList] {
	return func(ctx context.Context, in TenantRef, _ dashcontract.Principal) (ClientList, error) {
		s, err := services(sp)
		if err != nil {
			return ClientList{}, err
		}

		tid, err := liveTenant(ctx, s, in.TenantID)
		if err != nil {
			return ClientList{}, err
		}

		clients := s.Broker.Clients(tid)
		items := make([]Client, 0, len(clients))

		for _, c := range clients {
			items = append(items, fromClient(c))
		}

		return ClientList{Items: items}, nil
	}
}

func clientsKickHandler(sp ServicesProvider) handlerFunc[ClientRef, Ack] {
	return func(ctx context.Context, in ClientRef, _ dashcontract.Principal) (Ack, error) {
		s, err := services(sp)
		if err != nil {
			return Ack{}, err
		}

		tid, err := liveTenant(ctx, s, in.TenantID)
		if err != nil {
			return Ack{}, err
		}

		if err := required("clientId", in.ClientID); err != nil {
			return Ack{}, err
		}

		if err := s.Broker.Kick(tid, in.ClientID); err != nil {
			return Ack{}, brokerFail(s, "kick client", err)
		}

		return Ack{OK: true}, nil
	}
}

func sessionsDeleteHandler(sp ServicesProvider) handlerFunc[ClientRef, Ack] {
	return func(ctx context.Context, in ClientRef, _ dashcontract.Principal) (Ack, error) {
		s, err := services(sp)
		if err != nil {
			return Ack{}, err
		}

		tid, err := liveTenant(ctx, s, in.TenantID)
		if err != nil {
			return Ack{}, err
		}

		if err := required("clientId", in.ClientID); err != nil {
			return Ack{}, err
		}

		if err := s.Broker.DeleteSession(tid, in.ClientID); err != nil {
			return Ack{}, brokerFail(s, "delete session", err)
		}

		return Ack{OK: true}, nil
	}
}

func retainedListHandler(sp ServicesProvider) handlerFunc[RetainedListInput, RetainedList] {
	return func(ctx context.Context, in RetainedListInput, _ dashcontract.Principal) (RetainedList, error) {
		s, err := services(sp)
		if err != nil {
			return RetainedList{}, err
		}

		tid, err := liveTenant(ctx, s, in.TenantID)
		if err != nil {
			return RetainedList{}, err
		}

		filter := in.Filter
		if filter == "" {
			filter = retainedFilterDefault
		}

		// The broker lists nothing for a filter it rejects, which would
		// read as "no retained messages". Say why instead.
		if err := topic.Valid(filter, true, 0); err != nil {
			return RetainedList{}, mapErr(err)
		}

		infos := s.Broker.Retained(tid, filter)
		items := make([]Retained, 0, len(infos))

		for _, r := range infos {
			items = append(items, Retained{Topic: r.Topic, Size: r.Size, QoS: int(r.QoS)})
		}

		return RetainedList{Items: items}, nil
	}
}

func retainedDeleteHandler(sp ServicesProvider) handlerFunc[RetainedRef, Ack] {
	return func(ctx context.Context, in RetainedRef, _ dashcontract.Principal) (Ack, error) {
		s, err := services(sp)
		if err != nil {
			return Ack{}, err
		}

		tid, err := liveTenant(ctx, s, in.TenantID)
		if err != nil {
			return Ack{}, err
		}

		if err := required("topic", in.Topic); err != nil {
			return Ack{}, err
		}

		if err := s.Broker.DeleteRetained(tid, in.Topic); err != nil {
			return Ack{}, brokerFail(s, "delete retained", err)
		}

		return Ack{OK: true}, nil
	}
}
