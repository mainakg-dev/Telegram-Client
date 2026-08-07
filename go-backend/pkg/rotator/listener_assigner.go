package rotator

import (
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"telegram-client-backend/pkg/config"
	"telegram-client-backend/pkg/db"
	"telegram-client-backend/pkg/queue"
)

// AutoAssignListeners selects listener accounts and distributes target groups.
func AutoAssignListeners(targets []string, forceRebalance bool) map[string][]string {
	if targets == nil {
		targets = queue.Instance.GetActiveTargets()
	}

	if len(targets) == 0 {
		log.Println("📋 No active targets — clearing listener assignments")
		queue.Instance.SetListenerAssignments(make(map[string][]string))
		return make(map[string][]string)
	}

	numGroups := len(targets)
	neededListeners := calculateNeededListeners(numGroups)

	var allAccounts []db.Account
	db.DB.Order("id asc").Find(&allAccounts)

	healthyAccounts := filterHealthyAccounts(allAccounts)
	if len(healthyAccounts) == 0 {
		log.Println("❌ No healthy accounts available for listener assignment!")
		return make(map[string][]string)
	}

	if !forceRebalance {
		existing := queue.Instance.GetListenerAssignments()
		if len(existing) > 0 && isAssignmentValid(existing, targets, healthyAccounts) {
			log.Println("Existing listener assignments are valid — skipping rebalance")
			return existing
		}
	}

	// Priority: role == "LISTENER" first, then by ID ascending
	candidates := make([]db.Account, len(healthyAccounts))
	copy(candidates, healthyAccounts)
	sort.SliceStable(candidates, func(i, j int) bool {
		rI, rJ := 1, 1
		if candidates[i].Role == "LISTENER" {
			rI = 0
		}
		if candidates[j].Role == "LISTENER" {
			rJ = 0
		}
		if rI != rJ {
			return rI < rJ
		}
		return candidates[i].ID < candidates[j].ID
	})

	actualListeners := neededListeners
	if len(candidates) < actualListeners {
		actualListeners = len(candidates)
		log.Printf("⚠️ Need %d listeners for %d groups, but only %d healthy accounts available.", neededListeners, numGroups, actualListeners)
	}

	selected := candidates[:actualListeners]
	listenerSessions := make([]string, len(selected))
	healthyListenerSet := make(map[string]bool)
	assignments := make(map[string][]string)
	for i, acc := range selected {
		listenerSessions[i] = acc.SessionName
		healthyListenerSet[acc.SessionName] = true
		assignments[acc.SessionName] = make([]string, 0)
	}

	dbAssignments := db.GetAllListenerAssignments()

	// Clean up deleted target groups from SQLite
	targetSet := make(map[string]bool)
	for _, g := range targets {
		targetSet[g] = true
	}
	for deletedGroup := range dbAssignments {
		if !targetSet[deletedGroup] {
			db.DeleteListenerAssignment(deletedGroup)
		}
	}

	unassignedGroups := make([]string, 0)

	// Retain valid sticky DB assignments if session is in selected listeners
	for _, group := range targets {
		if assignedSession, ok := dbAssignments[group]; ok && healthyListenerSet[assignedSession] && !forceRebalance {
			assignments[assignedSession] = append(assignments[assignedSession], group)
		} else {
			unassignedGroups = append(unassignedGroups, group)
		}
	}

	// Round-robin assign remaining unassigned groups among selected listeners
	for idx, group := range unassignedGroups {
		sname := listenerSessions[idx%actualListeners]
		assignments[sname] = append(assignments[sname], group)
	}

	// Persist all listener assignments to SQLite DB
	for sname, groups := range assignments {
		for _, group := range groups {
			db.SaveListenerAssignment(group, sname)
		}
	}

	// Update DB roles
	updateAccountRolesInDB(listenerSessions, allAccounts)

	// Store in Redis
	queue.Instance.SetListenerAssignments(assignments)

	for sname, groups := range assignments {
		preview := groups
		if len(preview) > 3 {
			preview = preview[:3]
		}
		log.Printf("📋 Listener '%s' assigned %d groups: %v", sname, len(groups), preview)
	}

	log.Printf("✅ Listener assignment complete: %d listeners, %d groups", actualListeners, numGroups)
	return assignments
}

func calculateNeededListeners(numGroups int) int {
	if numGroups == 0 {
		return 0
	}
	needed := int(math.Ceil(float64(numGroups) / float64(queue.MaxGroupsPerListener)))
	if needed < 1 {
		return 1
	}
	return needed
}

func filterHealthyAccounts(accounts []db.Account) []db.Account {
	healthy := make([]db.Account, 0)
	sessionsDir := config.AppConfig.SessionsDir

	for _, acc := range accounts {
		if acc.Status == "UNAUTHORIZED" || acc.Status == "DISABLED" {
			continue
		}
		sessionPath := filepath.Join(sessionsDir, acc.SessionName+".session")
		if _, err := os.Stat(sessionPath); err == nil {
			healthy = append(healthy, acc)
		}
	}
	return healthy
}

func isAssignmentValid(existing map[string][]string, currentTargets []string, healthyAccounts []db.Account) bool {
	healthySet := make(map[string]bool)
	for _, acc := range healthyAccounts {
		healthySet[acc.SessionName] = true
	}

	for sname := range existing {
		if !healthySet[sname] {
			log.Printf("Listener '%s' is no longer healthy — triggering rebalance", sname)
			return false
		}
	}

	assignedTargets := make(map[string]bool)
	for _, groups := range existing {
		for _, g := range groups {
			assignedTargets[g] = true
		}
	}

	if len(assignedTargets) != len(currentTargets) {
		return false
	}
	for _, g := range currentTargets {
		if !assignedTargets[g] {
			return false
		}
	}

	return true
}

func updateAccountRolesInDB(listenerSessions []string, allAccounts []db.Account) {
	listenerSet := make(map[string]bool)
	for _, s := range listenerSessions {
		listenerSet[s] = true
	}

	for _, acc := range allAccounts {
		newRole := "REPLIER"
		if listenerSet[acc.SessionName] {
			newRole = "LISTENER"
		}
		if acc.Role != newRole {
			db.UpdateAccountRole(acc.ID, newRole)
		}
	}
}

func HandleListenerFailure(failedSessionName string) map[string][]string {
	log.Printf("🔄 Listener '%s' failed — triggering failover rebalance", failedSessionName)
	return AutoAssignListeners(nil, true)
}

// AutoAssignRepliers assigns target groups to REPLIER accounts for a specific worker.
// STRICT POLICY: No account is reused across different groups or sessions.
func AutoAssignRepliers(workerID string, targets []string, forceRebalance bool) map[string][]string {
	if targets == nil {
		targets = queue.Instance.GetActiveTargets()
	}

	if len(targets) == 0 {
		log.Printf("No active targets — clearing replier assignments for %s", workerID)
		queue.Instance.SetReplierAssignments(workerID, make(map[string][]string))
		queue.Instance.SetGroupPairAssignments(workerID, make(map[string]map[string]string))
		return make(map[string][]string)
	}

	var allAccounts []db.Account
	db.DB.Order("id asc").Find(&allAccounts)

	healthyAccounts := filterHealthyAccounts(allAccounts)
	repliers := make([]db.Account, 0)
	for _, acc := range healthyAccounts {
		if acc.Role == "REPLIER" || acc.Role == "" {
			repliers = append(repliers, acc)
		}
	}

	if len(repliers) == 0 {
		log.Printf("❌ No healthy replier accounts available for worker '%s'!", workerID)
		return make(map[string][]string)
	}

	replierSessionNames := make([]string, len(repliers))
	replierSet := make(map[string]bool)
	for i, acc := range repliers {
		replierSessionNames[i] = acc.SessionName
		replierSet[acc.SessionName] = true
	}

	dbAssignments := db.GetAllGroupAssignments()

	// Clean up deleted target groups from SQLite
	targetSet := make(map[string]bool)
	for _, g := range targets {
		targetSet[g] = true
	}
	for deletedGroup := range dbAssignments {
		if !targetSet[deletedGroup] {
			db.DeleteGroupAssignment(deletedGroup)
		}
	}

	pairMap := make(map[string]map[string]string)
	usedSessions := make(map[string]bool)

	// Pass 1: Retain valid sticky DB assignments (ensuring no session reuse)
	for _, group := range targets {
		if pairInfo, ok := dbAssignments[group]; ok {
			p := pairInfo["primary"]
			b := pairInfo["backup"]

			var validP, validB string
			if replierSet[p] && !usedSessions[p] {
				validP = p
				usedSessions[p] = true
			}
			if replierSet[b] && !usedSessions[b] && b != validP {
				validB = b
				usedSessions[b] = true
			}

			if validP != "" || validB != "" {
				pVal := validP
				bVal := validB
				if pVal == "" {
					pVal = validB
				}
				if bVal == "" {
					bVal = validP
				}
				pairMap[group] = map[string]string{
					"primary": pVal,
					"backup":  bVal,
				}
			}
		}
	}

	// Pass 2: Assign unassigned targets from remaining available sessions
	availableSessions := make([]string, 0)
	for _, s := range replierSessionNames {
		if !usedSessions[s] {
			availableSessions = append(availableSessions, s)
		}
	}

	for _, group := range targets {
		p := pairMap[group]["primary"]
		b := pairMap[group]["backup"]

		if p != "" && b != "" && p != b && !forceRebalance {
			continue
		}

		if p == "" {
			if len(availableSessions) > 0 {
				p = availableSessions[0]
				availableSessions = availableSessions[1:]
				usedSessions[p] = true
			}
		}

		if b == "" || b == p {
			if len(availableSessions) > 0 {
				b = availableSessions[0]
				availableSessions = availableSessions[1:]
				usedSessions[b] = true
			} else {
				b = p
			}
		}

		if p != "" {
			pairMap[group] = map[string]string{
				"primary": p,
				"backup":  b,
			}
			db.SaveGroupAssignment(group, p, b)
		} else {
			log.Printf("❌ Cannot assign group '%s': All replier accounts used (Strict 1-account-per-group policy).", group)
		}
	}

	// Reverse assignment map: session_name -> []group
	assignments := make(map[string][]string)
	for group, pair := range pairMap {
		pName := pair["primary"]
		bName := pair["backup"]
		if pName != "" {
			assignments[pName] = append(assignments[pName], group)
		}
		if bName != "" && bName != pName {
			assignments[bName] = append(assignments[bName], group)
		}
	}

	cleanAssignments := make(map[string][]string)
	for k, v := range assignments {
		if len(v) > 0 {
			cleanAssignments[k] = v
		}
	}

	queue.Instance.SetReplierAssignments(workerID, cleanAssignments)
	queue.Instance.SetGroupPairAssignments(workerID, pairMap)

	log.Printf("✅ Strict non-reusing replier assignment for '%s': %d groups assigned to %d unique replier sessions.", workerID, len(pairMap), len(usedSessions))
	return cleanAssignments
}

func isReplierAssignmentValid(existing map[string][]string, currentTargets []string) bool {
	assigned := make(map[string]bool)
	for _, groups := range existing {
		for _, g := range groups {
			assigned[g] = true
		}
	}

	if len(assigned) != len(currentTargets) {
		return false
	}
	for _, g := range currentTargets {
		if !assigned[g] {
			return false
		}
	}
	return true
}

func BuildTargetToReplierMap(assignments map[string][]string) map[string]string {
	reverseMap := make(map[string]string)
	for sessionName, groups := range assignments {
		for _, group := range groups {
			if _, ok := reverseMap[group]; !ok {
				reverseMap[group] = sessionName
			}
		}
	}
	return reverseMap
}

func GetListenerSummary() map[string]interface{} {
	listenerAssignments := queue.Instance.GetListenerAssignments()
	replierAssignments := queue.Instance.GetAllReplierAssignments()
	targets := queue.Instance.GetActiveTargets()

	listenerSummary := make(map[string]interface{})
	for sname, groups := range listenerAssignments {
		listenerSummary[sname] = map[string]interface{}{
			"group_count": len(groups),
			"groups":      groups,
		}
	}

	replierSummary := make(map[string]interface{})
	for workerID, workerAssigns := range replierAssignments {
		wMap := make(map[string]interface{})
		for sname, groups := range workerAssigns {
			wMap[sname] = map[string]interface{}{
				"group_count": len(groups),
				"groups":      groups,
			}
		}
		replierSummary[workerID] = wMap
	}

	return map[string]interface{}{
		"total_targets":           len(targets),
		"total_listeners":         len(listenerAssignments),
		"max_groups_per_listener": queue.MaxGroupsPerListener,
		"listener_assignments":   listenerSummary,
		"replier_assignments":    replierSummary,
	}
}
